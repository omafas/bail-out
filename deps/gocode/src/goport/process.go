// @author Couchbase <info@couchbase.com>
// @copyright 2018-2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
)

const (
	// StreamStdin is a constant used to designate process' standard
	// input.
	StreamStdin = "stdin"
	// StreamStdout is a constant used to designate process' standard
	// output.
	StreamStdout = "stdout"
	// StreamStderr is a constant used to designate process' standard
	// error.
	StreamStderr = "stderr"
)

var (
	// ErrClosed is an error returned by Process methods on attempt to do
	// something with a stream that is already closed.
	ErrClosed = errors.New("closed")
)

// Process is a thin wrapper around exec.Cmd and the pipes to communicate with
// the process represented by the exec.Cmd.
type Process struct {
	cmd *exec.Cmd

	stdinMu *sync.Mutex
	stdin   io.WriteCloser

	stdout io.ReadCloser
	stderr io.ReadCloser

	waitErr error
	waitCh  chan struct{}
}

// StartProcess starts a process given a path and a list of arguments to pass
// to the executable.
func StartProcess(path string, args []string) (*Process, error) {
	// files we'll need after fork
	keepFiles := ([]io.Closer)(nil)

	// files we need to close after fork
	closeFiles := ([]io.Closer)(nil)

	defer func() {
		files := append(keepFiles, closeFiles...)
		closeAll(files)
	}()

	keepFile := func(file io.Closer) {
		keepFiles = append(keepFiles, file)
	}

	closeFile := func(file io.Closer) {
		closeFiles = append(closeFiles, file)
	}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	keepFile(stdinW)
	closeFile(stdinR)

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	keepFile(stdoutR)
	closeFile(stdoutW)

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	keepFile(stderrR)
	closeFile(stderrW)

	cmd := exec.Command(path, args...)
	cmd.Stdin = stdinR
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	SetPgid(cmd)

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// don't close fds that we need
	keepFiles = nil

	p := &Process{
		cmd: cmd,

		stdinMu: &sync.Mutex{},
		stdin:   &CloseOnce{File: stdinW},

		stdout: stdoutR,
		stderr: stderrR,
	}

	p.startWaiter()

	return p, nil
}

func (p *Process) startWaiter() {
	ch := make(chan struct{})

	go func() {
		p.waitErr = p.wait()
		close(ch)
	}()

	p.waitCh = ch
}

func (p *Process) wait() error {
	err := p.cmd.Wait()
	if p.cmd.ProcessState != nil {
		err = nil
	}

	return err
}

// Write writes to the standard input of the process.
func (p *Process) Write(data []byte) (int, error) {
	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()

	if p.stdin == nil {
		return 0, ErrClosed
	}

	return p.stdin.Write(data)
}

// Close closes the standard input of the underlying process.
func (p *Process) Close() error {
	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()

	if p.stdin == nil {
		return ErrClosed
	}

	err := p.stdin.Close()
	p.stdin = nil

	return err
}

// GetStdout returns an io.Reader that can be used to read from the process'
// standard output.
func (p *Process) GetStdout() io.Reader {
	return p.stdout
}

// GetStderr returns an io.Reader that can be used to read from the process'
// standard error.
func (p *Process) GetStderr() io.Reader {
	return p.stderr
}

// Kill kills the process brutally.
func (p *Process) Kill() error {
	return KillPgroup(p.cmd)
}

// Wait waits for the process to terminate.
func (p *Process) Wait() error {
	<-p.waitCh
	return p.waitErr
}

// GetExitStatus returns an exit status of the process once it has
// terminated. Needs to be called after Wait.
func (p *Process) GetExitStatus() int {
	return GetExitStatus(p.cmd)
}

func closeAll(files []io.Closer) {
	for _, f := range files {
		f.Close()
	}
}

// ProcessWorker provides a higher level API around the Process.
type ProcessWorker struct {
	process *Process

	stdinWriter  *AsyncWriter
	stdoutReader *AsyncReader
	stderrReader *AsyncReader

	events     chan interface{}
	observerCh chan error

	activeStreams map[string]*AsyncReader

	err error

	*Canceler
}

// ProcessStreamData is returned via the events channel whenever the process
// writes something on either standard output or standard error.
type ProcessStreamData struct {
	// Stream is either StreamStdout or StreamStderr.
	Stream string
	// Data is that data that was produced on the corresponding stream.
	Data []byte
}

// ProcessStreamError is returned via the events channel whenever the
// ProcessWorker encounters an error while reading from any of the process'
// streams.
type ProcessStreamError struct {
	// Stream is either StreamStdout or StreamStderr.
	Stream string
	// Error indicates the nature of the error. Most commonly, io.EOF.
	Error error
}

// ProcessExited is returned via the events channel when the process
// exits. It's always the last message returned before the events channel is
// closed (unless something abnormal happens, in which case, there might not
// be ProcessExited event queued at all).
type ProcessExited struct {
	// Status is the exit code with which the process has exited.
	Status int
}

// NewProcessWorker creates a worker for the passed Process.
func NewProcessWorker(process *Process) *ProcessWorker {
	w := &ProcessWorker{
		process:  process,
		events:   make(chan interface{}),
		Canceler: NewCanceler(),
	}

	w.startWorkers()
	w.startObserver()

	go w.loop()

	return w
}

// GetError returns an error which caused the abnormal termination of the
// ProcessWorker. It can only safely be used once the events channel was
// closed.
func (w *ProcessWorker) GetError() error {
	return w.err
}

// GetEventsChan returns the channel used to expose read/exit events about the
// corresponding process.
func (w *ProcessWorker) GetEventsChan() <-chan interface{} {
	return w.events
}

// Close closes a standard input of the underlying process.
func (w *ProcessWorker) Close() error {
	w.stdinWriter.Cancel()
	w.stdinWriter.Wait()

	return w.process.Close()
}

// Write writes to the standard input of the underlying process.
func (w *ProcessWorker) Write(data []byte) <-chan error {
	return w.stdinWriter.Writev(data)
}

func (w *ProcessWorker) loop() {
	defer w.Follower().Done()
	defer w.terminateWorkers()
	defer close(w.events)

	for {
		select {
		case <-w.Follower().Cancel():
			w.err = ErrCanceled
			return
		case data, ok := <-w.getStream(StreamStdout):
			w.handleRead(StreamStdout, data, ok)
		case data, ok := <-w.getStream(StreamStderr):
			w.handleRead(StreamStderr, data, ok)
		case err := <-w.getChildDone():
			if err != nil {
				w.err = err
				return
			}

			ok := w.handleProcessExit()
			if ok {
				return
			}
		}
	}
}

func (w *ProcessWorker) startWorkers() {
	w.stdinWriter = NewAsyncWriter(&SimpleVectorWriter{w.process})
	w.stdoutReader = NewAsyncReader(w.process.GetStdout())
	w.stderrReader = NewAsyncReader(w.process.GetStderr())

	w.activeStreams = make(map[string]*AsyncReader)
	w.activeStreams[StreamStdout] = w.stdoutReader
	w.activeStreams[StreamStderr] = w.stderrReader
}

func (w *ProcessWorker) terminateWorkers() {
	w.stdinWriter.Cancel()
	w.stdoutReader.Cancel()
	w.stderrReader.Cancel()

	w.stdinWriter.Wait()
	w.stdoutReader.Wait()
	w.stderrReader.Wait()
}

func (w *ProcessWorker) getStream(stream string) <-chan []byte {
	reader, ok := w.activeStreams[stream]
	if !ok {
		return nil
	}

	return reader.GetReadChan()
}

func (w *ProcessWorker) startObserver() {
	ch := make(chan error)

	go func() {
		ch <- w.process.Wait()
		close(ch)
	}()

	w.observerCh = ch
}

func (w *ProcessWorker) getChildDone() <-chan error {
	// this makes sure that stderr and stdout are read to completion
	// before we report the child dead
	if len(w.activeStreams) != 0 {
		return nil
	}

	return w.observerCh
}

func (w *ProcessWorker) handleRead(stream string, data []byte, ok bool) {
	if !ok {
		reader := w.activeStreams[stream]
		delete(w.activeStreams, stream)

		event := &ProcessStreamError{
			Stream: stream,
			Error:  reader.GetError(),
		}
		w.queueEvent(event)
		return
	}

	event := &ProcessStreamData{
		Stream: stream,
		Data:   data,
	}
	w.queueEvent(event)
}

func (w *ProcessWorker) handleProcessExit() bool {
	event := &ProcessExited{
		Status: w.process.GetExitStatus(),
	}
	return w.queueEvent(event)
}

func (w *ProcessWorker) queueEvent(event interface{}) bool {
	select {
	case w.events <- event:
		return true
	case <-w.Follower().Cancel():
		return false
	}
}
