// @author Couchbase <info@couchbase.com>
// @copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

var (
	// ErrCanceled is an error used as indication that operation of
	// interest was canceled.
	ErrCanceled = errors.New("canceled")
)

// Canceler provides a generic way to cancel and wait for termination of an
// active object (object that is backed by a goroutine).
type Canceler struct {
	cancel     chan struct{}
	cancelOnce *sync.Once

	done     chan struct{}
	doneOnce *sync.Once
}

// CancelFollower exposes APIs that must only be used by the active object
// itself.
type CancelFollower Canceler

// NewCanceler creates a new Canceler object.
func NewCanceler() *Canceler {
	return &Canceler{
		cancel:     make(chan struct{}),
		cancelOnce: &sync.Once{},

		done:     make(chan struct{}),
		doneOnce: &sync.Once{},
	}
}

// Cancel orders the underlying active object to cancel whatever its doing.
func (c *Canceler) Cancel() {
	c.cancelOnce.Do(func() { close(c.cancel) })
}

// Wait waits for the active object to finish.
func (c *Canceler) Wait() {
	<-c.done
}

// Follower can be used by the active object to gain access to the
// active-object-only interface to of the Canceler.
func (c *Canceler) Follower() *CancelFollower {
	return (*CancelFollower)(c)
}

// Done indicates that the active object finished its operation.
func (f *CancelFollower) Done() {
	f.doneOnce.Do(func() { close(f.done) })
}

// Cancel returns a channel that the active object should monitor for the
// orders to cancel its operation.
func (f *CancelFollower) Cancel() <-chan struct{} {
	return f.cancel
}

// CloseOnce wraps an os.File and ensures that the file is only closed once.
type CloseOnce struct {
	*os.File
	once sync.Once
}

// Close closes the underlying file. But only on the first
// invocation. Subsequent invocations simply return success.
func (c *CloseOnce) Close() error {
	var err error
	c.once.Do(func() { err = c.File.Close() })

	return err
}

// SetPgid changes exec.Cmd attributes to start the process in its own process
// group.
func SetPgid(cmd *exec.Cmd) {
	doSetPgid(cmd)
}

// KillPgroup kills an entire process group if the platform supports this.
func KillPgroup(cmd *exec.Cmd) error {
	return doKillPgroup(cmd)
}

// different platforms define different types to represent process wait
// status, but most of them have these methods
type processStatus interface {
	Exited() bool
	Signaled() bool
	Signal() syscall.Signal
	ExitStatus() int
}

// GetExitStatus returns an exit status of a terminated process.
func GetExitStatus(cmd *exec.Cmd) int {
	status, ok := cmd.ProcessState.Sys().(processStatus)

	if !ok {
		if cmd.ProcessState.Success() {
			return 0
		}

		return 1
	}

	if !status.Signaled() && !status.Exited() {
		panic("process neither exited nor signaled")
	}

	if status.Signaled() {
		sig := status.Signal()
		// convert to exit status the way Linux does it
		return 128 + int(sig)
	}

	// exited
	return status.ExitStatus()
}
