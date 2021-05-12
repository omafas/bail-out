// @author Couchbase <info@couchbase.com>
// @copyright 2018-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import "bytes"

// Op represents a decoded operation.
type Op struct {
	// Name is the name of the operation.
	Name string
	// Arg (can be nil) is a supplied argument to the operation.
	Arg []byte
}

// OpsReader decodes a stream of operations from an underlying PacketReader.
type OpsReader struct {
	packetStream PacketReader

	ops chan *Op
	err error

	*Canceler
}

// NewOpsReader creates an OpsReader that reads from the provided
// PacketReader. Each packet must correspond to one operation.
func NewOpsReader(stream PacketReader) *OpsReader {
	ops := make(chan *Op)

	or := &OpsReader{
		packetStream: stream,

		ops: ops,
		err: nil,

		Canceler: NewCanceler(),
	}

	go or.loop()
	return or
}

// GetOpsChan returns a channel where the decoded operations are put into.
func (or *OpsReader) GetOpsChan() <-chan *Op {
	return or.ops
}

// GetError returns a reason for OpsReader termination. Can only be called
// safely once the channel returned by GetOpsChan was closed.
func (or *OpsReader) GetError() error {
	return or.err
}

func (or *OpsReader) loop() {
	defer or.Follower().Done()

	for {
		var packet []byte
		done := make(chan error)

		go func() {
			var err error
			packet, err = or.packetStream.ReadPacket()

			done <- err
			close(done)
		}()

		var err error

		select {
		case <-or.Follower().Cancel():
			err = ErrCanceled
		case err = <-done:
			if err == nil {
				op := or.parseOp(packet)
				select {
				case or.ops <- op:
				case <-or.Follower().Cancel():
					err = ErrCanceled
				}
			}
		}

		if err != nil {
			or.err = err
			close(or.ops)
			return
		}
	}
}

func (or *OpsReader) parseOp(packet []byte) *Op {
	switch i := bytes.IndexByte(packet, ':'); i {
	case -1:
		return &Op{string(packet), nil}
	default:
		name := string(packet[0:i])
		arg := packet[i+1:]
		if len(arg) == 0 {
			arg = nil
		}

		return &Op{name, arg}
	}
}
