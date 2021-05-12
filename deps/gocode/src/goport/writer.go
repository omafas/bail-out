// @author Couchbase <info@couchbase.com>
// @copyright 2018-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"io"
)

// VectorWriter is an interface for objects that support writing a "vector" of
// data chunks without having to concatenate the chunks first.
type VectorWriter interface {
	// Writev writes a vector of data chunks to underlying object. If
	// error is returned, no assumptions are made about the amount of data
	// that actually was written.
	Writev(...[]byte) error
}

// SimpleVectorWriter wraps any io.Writer and provides VectorWriter interface.
type SimpleVectorWriter struct {
	io.Writer
}

// Writev writes a vector of data chunks to underlying io.Writer.
func (w *SimpleVectorWriter) Writev(data ...[]byte) error {
	for _, chunk := range data {
		_, err := w.Write(chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

// AsyncWriter writes to an underlying VectorWriter in a goroutine.
type AsyncWriter struct {
	jobs chan *write
	dst  VectorWriter

	*Canceler
}

type write struct {
	data [][]byte
	err  chan error
}

// NewAsyncWriter creates an AsyncWriter that writes to a provided
// VectorWriter.
func NewAsyncWriter(dst VectorWriter) *AsyncWriter {
	w := &AsyncWriter{
		jobs:     make(chan *write),
		dst:      dst,
		Canceler: NewCanceler(),
	}

	go w.loop()
	return w
}

func (w *AsyncWriter) loop() {
	defer w.Follower().Done()

	for {
		select {
		case job := <-w.jobs:
			done := make(chan error, 1)

			go func() {
				done <- w.dst.Writev(job.data...)
				close(done)
			}()

			var err error
			stop := false

			select {
			case <-w.Follower().Cancel():
				err = ErrCanceled
				stop = true
			case err = <-done:
			}

			job.err <- err
			close(job.err)

			if stop {
				return
			}

		case <-w.Follower().Cancel():
			return
		}
	}
}

// Writev submits a vector of data chunks to be written to AsyncWriter. When
// the write is completed the result is put into the returned channel.
func (w *AsyncWriter) Writev(data ...[]byte) <-chan error {
	err := make(chan error, 1)
	job := &write{data, err}

	go func() {
		select {
		case w.jobs <- job:
			return
		case <-w.Follower().Cancel():
			err <- ErrCanceled
			close(err)
			// loop will call Done
		}
	}()

	return err
}
