// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"

	"gocbutils"
)

var (
	errProcessExited = errors.New("process exited")
)

type portSpec struct {
	cmd  string
	args []string

	windowSize       int
	interactive      bool
	gracefulShutdown bool
}

type processState int

const (
	processStateRunning      = processState(0)
	processStateShuttingDown = iota
	processStateExited       = iota
)

type loopState struct {
	unackedBytes int
	pendingWrite <-chan error

	pendingOp            string
	pendingOpChan        <-chan error
	pendingOpResultValue string

	processState processState
}

type port struct {
	child       *Process
	childWorker *ProcessWorker
	childSpec   portSpec

	opsReader *OpsReader

	parentWriter       *AsyncWriter
	parentWriterStream *NetStringWriter

	state loopState
}

func newPort(spec portSpec) *port {
	return &port{childSpec: spec}
}

func (p *port) startChild() error {
	child, err := StartProcess(p.childSpec.cmd, p.childSpec.args)
	if err != nil {
		return err
	}

	p.child = child
	return nil
}

func (p *port) startWorkers() {
	packetReader := (PacketReader)(NewNetStringReader(os.Stdin))
	if p.childSpec.interactive {
		packetReader = newInteractiveReader()
	}

	p.opsReader = NewOpsReader(packetReader)

	p.parentWriterStream = NewNetStringWriter(os.Stdout)
	p.parentWriter = NewAsyncWriter(p.parentWriterStream)

	p.childWorker = NewProcessWorker(p.child)
}

func (p *port) terminateWorkers() {
	p.opsReader.Cancel()
	p.parentWriter.Cancel()
	p.childWorker.Cancel()

	p.opsReader.Wait()
	p.parentWriter.Wait()
	p.childWorker.Wait()
}

func (p *port) initLoopState() {
	p.state.unackedBytes = 0
	p.state.pendingWrite = nil
	p.state.processState = processStateRunning

	p.state.pendingOp = ""
	p.state.pendingOpChan = nil
	p.state.pendingOpResultValue = ""
}

func (p *port) getOpsChan() <-chan *Op {
	if p.state.pendingOpChan != nil {
		return nil
	}

	return p.opsReader.GetOpsChan()
}

func (p *port) getPendingOpChan() <-chan error {
	if p.state.pendingWrite != nil {
		return nil
	}

	return p.state.pendingOpChan
}

func (p *port) isWindowFull() bool {
	return p.state.unackedBytes >= p.childSpec.windowSize
}

func (p *port) isFlowControlBlocked() bool {
	// shutdown request implicitly acks everything
	if p.isShuttingDown() {
		return false
	}

	// This is a stop-gap measure to prevent deadlocks if the supervised
	// process stops reading from its stdin if it can't write to
	// stdout/stderr. Since there's currently no way to acknowledge reads
	// while there's a blocked operation, we have to open the flow control
	// gate.
	if p.state.pendingOp == "write" {
		return false
	}

	return p.isWindowFull()
}

func (p *port) getChildEventsChan() <-chan interface{} {
	if p.state.pendingWrite != nil ||
		p.hasProcessExited() ||
		p.isFlowControlBlocked() {
		return nil
	}

	return p.childWorker.GetEventsChan()
}

func (p *port) parentWrite(data ...[]byte) {
	if p.state.pendingWrite != nil {
		panic("pending write is non-nil")
	}

	p.state.pendingWrite = p.parentWriter.Writev(data...)
}

func (p *port) parentSyncWrite(data ...[]byte) error {
	if p.state.pendingWrite != nil {
		panic("pending write is non-nil")
	}

	return <-p.parentWriter.Writev(data...)
}

func (p *port) handleChildEvent(event interface{}) error {
	switch event.(type) {
	case *ProcessStreamData:
		p.handleStreamData(event.(*ProcessStreamData))
	case *ProcessStreamError:
		p.handleStreamError(event.(*ProcessStreamError))
	case *ProcessExited:
		return p.handleProcessExited(event.(*ProcessExited))
	default:
		panic(fmt.Errorf("unknown event %v", event))
	}

	return nil
}

func (p *port) handleStreamData(event *ProcessStreamData) {
	p.state.unackedBytes += len(event.Data)
	p.parentWrite([]byte(event.Stream), []byte(":"), event.Data)
}

func (p *port) handleStreamError(event *ProcessStreamError) {
	msg := [][]byte{[]byte("eof:"), []byte(event.Stream)}
	if event.Error != io.EOF {
		msg = append(msg, []byte(":"), []byte(event.Error.Error()))
	}

	p.parentWrite(msg...)
}

func (p *port) handleProcessExited(event *ProcessExited) error {
	status := event.Status
	if p.isShuttingDown() {
		// respond to the shutdown request
		err := p.handleOpResult(nil)
		if err != nil {
			return err
		}

		p.noteOpDone()

		// Since we know that the child process is getting shutdown,
		// we can set the exit code to 0. Note that this would hide
		// the actual exit code if the child died before it could
		// handle our SIGKILL. But this is the best we can do as that
		// window would always exist.
		status = 0
	}

	p.noteProcessExited()
	p.parentWrite([]byte("exit:"), []byte(strconv.Itoa(status)))

	return nil
}

func (p *port) handleOp(op *Op) {
	var ch <-chan error

	switch {
	case p.hasProcessExited():
		ch = p.handleOpNoProcess()
	case op.Name == "ack":
		ch = p.handleAck(op.Arg)
	case op.Name == "write":
		ch = p.handleWrite(op.Arg)
	case op.Name == "close":
		ch = p.handleCloseStream(op.Arg)
	case op.Name == "shutdown":
		ch = p.handleShutdown()
	case op.Name == "get_child_os_pid":
		ch = p.handleGetChildOsPid()
	default:
		ch = p.handleUnknown(op.Name)
	}

	p.state.pendingOp = op.Name
	p.state.pendingOpChan = ch
}

func (p *port) handleOpNoProcess() <-chan error {
	ch := make(chan error, 1)
	ch <- errProcessExited

	return ch
}

func (p *port) handleShutdown() <-chan error {
	ch := make(chan error, 1)

	err := p.shutdown()
	if err != nil {
		ch <- err
		return ch
	}

	p.noteShuttingDown()

	// Note that the channel is empty. The operation is responded to once
	// we see the child terminate.
	return ch
}

func (p *port) handleGetChildOsPid() <-chan error {
	ch := make(chan error, 1)
	p.state.pendingOpResultValue = strconv.Itoa(p.child.pid)
	ch <- nil
	return ch
}

func (p *port) shutdown() error {
	var err error
	if p.childSpec.gracefulShutdown {
		err = p.child.Close()

		// The error can be ErrClosed when the user closed stdin
		// before calling shutdown and we still haven't received an
		// event indicating that the process has died. Assuming the
		// supervised process behaves correctly, it will eventually
		// die. So we can ignore this error for purposes of shutdown.
		if err == ErrClosed {
			err = nil
		}
	} else {
		err = p.child.Kill()
	}

	return err
}

func (p *port) shutdownAndWait() {
	if p.shutdown() == nil {
		p.child.Wait()
	}
}

func (p *port) handleWrite(data []byte) <-chan error {
	return p.childWorker.Write(data)
}

func (p *port) handleAck(data []byte) <-chan error {
	ch := make(chan error, 1)

	if len(data) == 0 {
		ch <- fmt.Errorf("need argument")
		return ch
	}

	count, err := strconv.Atoi(string(data))
	if err != nil {
		ch <- fmt.Errorf("invalid value '%s'", string(data))
		return ch
	}

	p.state.unackedBytes -= count
	if p.state.unackedBytes < 0 {
		p.state.unackedBytes = 0
	}

	ch <- nil
	return ch
}

func (p *port) handleCloseStream(data []byte) <-chan error {
	ch := make(chan error, 1)
	stream := string(data)

	switch stream {
	case StreamStdin:
		err := p.childWorker.Close()
		ch <- err
	default:
		ch <- fmt.Errorf("unknown stream '%s'", stream)
	}

	return ch
}

func (p *port) handleUnknown(cmd string) <-chan error {
	ch := make(chan error, 1)
	ch <- fmt.Errorf("unknown command '%s'", cmd)

	return ch
}

func (p *port) handleOpResult(err error) error {
	resp := "ok"

	if p.state.pendingOpResultValue != "" {
		resp = fmt.Sprintf("ok:%s", p.state.pendingOpResultValue)
	}

	if err != nil {
		resp = fmt.Sprintf("error:%s", err.Error())
	}

	return p.parentSyncWrite([]byte(resp))
}

func (p *port) noteOpDone() {
	p.state.pendingOp = ""
	p.state.pendingOpChan = nil
	p.state.pendingOpResultValue = ""
}

func (p *port) noteWriteDone() {
	p.state.pendingWrite = nil
}

func (p *port) noteShuttingDown() {
	p.state.processState = processStateShuttingDown
}

func (p *port) isShuttingDown() bool {
	return p.state.processState == processStateShuttingDown
}

func (p *port) noteProcessExited() {
	p.state.processState = processStateExited
}

func (p *port) hasProcessExited() bool {
	return p.state.processState == processStateExited
}

func (p *port) loop() error {
	err := p.startChild()
	if err != nil {
		return fmt.Errorf("failed to start child: %s", err.Error())
	}
	defer p.shutdownAndWait()

	p.startWorkers()
	defer p.terminateWorkers()

	p.initLoopState()

	for {
		select {
		case op, ok := <-p.getOpsChan():
			if !ok {
				err := p.opsReader.GetError()
				if err == io.EOF {
					return nil
				}

				return fmt.Errorf("read failed: %s", err)
			}

			p.handleOp(op)
		case err := <-p.getPendingOpChan():
			err = p.handleOpResult(err)
			p.noteOpDone()
			if err != nil {
				return fmt.Errorf(
					"failed to write to parent: %s",
					err.Error())
			}
		case event, ok := <-p.getChildEventsChan():
			if !ok {
				return fmt.Errorf(
					"unexpected child error: %v",
					p.childWorker.GetError())
			}

			err = p.handleChildEvent(event)
			if err != nil {
				return err
			}
		case err := <-p.state.pendingWrite:
			p.noteWriteDone()
			if err != nil {
				return fmt.Errorf(
					"parent write failed: %s", err.Error())
			}
		}
	}
}

type argsFlag []string

func (args *argsFlag) String() string {
	return fmt.Sprintf("%v", *args)
}

func (args *argsFlag) Set(v string) error {
	*args = append(*args, v)
	return nil
}

type cmdFlag string

func (c *cmdFlag) String() string {
	return string(*c)
}

func (c *cmdFlag) Set(v string) error {
	if v == "" {
		return errors.New("-cmd can't be empty")
	}

	if path.IsAbs(v) {
		*c = cmdFlag(v)
	} else {
		abs, err := exec.LookPath(v)
		if err != nil {
			return fmt.Errorf(
				"failed to find '%s' in path: %s",
				v,
				err.Error())
		}

		*c = cmdFlag(abs)
	}

	return nil
}

func getCmdFromEnv() (string, []string) {
	rawArgs := os.Getenv("GOPORT_ARGS")
	if rawArgs == "" {
		log.Fatalf("GOPORT_ARGS is empty")
	}

	var args []string

	err := json.Unmarshal([]byte(rawArgs), &args)
	if err != nil {
		log.Fatalf(
			"couldn't unmarshal GOPORT_ARGS(%s): %s",
			rawArgs,
			err.Error())
	}

	if len(args) < 1 {
		log.Fatalf("missing executable")
	}

	return args[0], args[1:]
}

type interactiveReader struct {
	scanner *bufio.Scanner
}

func newInteractiveReader() *interactiveReader {
	return &interactiveReader{bufio.NewScanner(os.Stdin)}
}

func (r *interactiveReader) ReadPacket() ([]byte, error) {
	read := r.scanner.Scan()
	if read {
		return r.scanner.Bytes(), nil
	}

	err := r.scanner.Err()
	if err != nil {
		return nil, err
	}

	return nil, io.EOF
}

func main() {
	var windowSize int

	var cmd string
	var args []string

	var interactive bool
	var gracefulShutdown bool

	flag.IntVar(&windowSize, "window-size", 64*1024, "window size")
	flag.BoolVar(&interactive, "interactive", false,
		"run in interactive mode")
	flag.BoolVar(&gracefulShutdown, "graceful-shutdown", false,
		"terminate supervised gracefully by closing its stdin")
	flag.Var((*cmdFlag)(&cmd), "cmd", "command to execute")
	flag.Var((*argsFlag)(&args), "args", "command arguments")
	flag.Parse()

	log.SetPrefix("[goport] ")

	if windowSize < 0 {
		log.Fatalf("window size can't be less than zero")
	}

	if cmd == "" {
		cmd, args = getCmdFromEnv()
	}

	log.SetPrefix(fmt.Sprintf("[goport(%s)] ", cmd))

	gocbutils.LimitCPUThreads()

	port := newPort(portSpec{
		cmd:              cmd,
		args:             args,
		windowSize:       windowSize,
		interactive:      interactive,
		gracefulShutdown: gracefulShutdown,
	})

	err := port.loop()
	if err != nil {
		log.Fatal(err)
	}
}
