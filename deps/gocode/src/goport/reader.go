// @author Couchbase <info@couchbase.com>
// @copyright 2018 Couchbase, Inc.
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
	"bufio"
	"io"
)

const (
	bufferSize = 64 * 1024
)

// AsyncReader reads from a provided io.Reader in a goroutine and exposes the
// read data via a channel.
type AsyncReader struct {
	stream *bufio.Reader

	reads chan []byte
	err   error

	*Canceler
}

// NewAsyncReader creates a AsyncReader for a given io.Reader.
func NewAsyncReader(stream io.Reader) *AsyncReader {
	reads := make(chan []byte)

	r := &AsyncReader{
		stream:   bufio.NewReaderSize(stream, bufferSize),
		reads:    reads,
		err:      nil,
		Canceler: NewCanceler(),
	}

	go r.loop()
	return r
}

// GetReadChan returns a channel that the AsyncReader will write the
// underlying data to.
func (r *AsyncReader) GetReadChan() <-chan []byte {
	return r.reads
}

// GetError returns an error indicating the reason why AsyncReader
// terminated. It's only safe to call this method once the channel returned
// by GetReadChan is closed.
func (r *AsyncReader) GetError() error {
	return r.err
}

func (r *AsyncReader) loop() {
	defer r.Follower().Done()

	buffer := make([]byte, bufferSize)
	for {
		done := make(chan struct {
			n   int
			err error
		}, 1)

		go func() {
			n, err := r.stream.Read(buffer)
			done <- struct {
				n   int
				err error
			}{n, err}
			close(done)
		}()

		var err error
		select {
		case <-r.Follower().Cancel():
			err = ErrCanceled
		case result := <-done:
			if result.n != 0 {
				cp := make([]byte, result.n)
				copy(cp, buffer)

				select {
				case r.reads <- cp:
				case <-r.Follower().Cancel():
					err = ErrCanceled
				}
			}

			err = result.err
		}

		if err != nil {
			r.err = err
			close(r.reads)
			return
		}
	}
}
