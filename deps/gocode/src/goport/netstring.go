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
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// PacketReader defines an interface for sources of data that have packetized
// nature.
type PacketReader interface {
	// ReadPacket reads a single complete packet from the data source.
	ReadPacket() ([]byte, error)
}

var (
	// ErrInvalidNetString is returned by the NetStringReader when it
	// cannot decode the data from an underlying io.Reader.
	ErrInvalidNetString = errors.New("invalid netstring data")
)

// NetStringReader reads packets of data encoded as netstrings from io.Reader.
type NetStringReader struct {
	reader *bufio.Reader
}

// NewNetStringReader creates a NetStringReader that reads from the provided
// io.Reader.
func NewNetStringReader(r io.Reader) *NetStringReader {
	return &NetStringReader{bufio.NewReader(r)}
}

// ReadPacket reads a single netstring encoded packet from the provided
// io.Reader.
func (n *NetStringReader) ReadPacket() ([]byte, error) {
	rawLength, err := n.reader.ReadSlice(':')
	switch err {
	case nil:
		// ok
	case bufio.ErrBufferFull:
		// we couldn't find the colon before buffer filled in;
		// consider this an invalid input
		return nil, ErrInvalidNetString
	default:
		return nil, err
	}

	var length uint
	lengthStr := strings.TrimSpace(string(rawLength))

	_, err = fmt.Sscanf(lengthStr, "%9d:", &length)
	if err != nil {
		return nil, ErrInvalidNetString
	}

	toRead := int(length) + 1
	packet := make([]byte, toRead)
	read, err := io.ReadFull(n.reader, packet)

	switch {
	case read != toRead && err != nil:
		return nil, err
	case read != toRead:
		return nil, ErrInvalidNetString
	case packet[length] != ',':
		return nil, ErrInvalidNetString
	default:
		// err might EOF, which is normal
		return packet[:length], err
	}
}

// NetStringWriter encodes packets of data as netstrings and writes the
// results to the specified io.Writer.
type NetStringWriter struct {
	writer *bufio.Writer
}

// NewNetStringWriter creates a NetStringWriter that writes to the provided
// io.Writer.
func NewNetStringWriter(w io.Writer) *NetStringWriter {
	return &NetStringWriter{bufio.NewWriter(w)}
}

// Writev writes a sequence of data chunks as a single packet.
func (n *NetStringWriter) Writev(data ...[]byte) error {
	totalLen := 0
	for _, chunk := range data {
		totalLen += len(chunk)
	}

	_, err := n.writer.WriteString(strconv.Itoa(totalLen))
	if err != nil {
		return err
	}

	err = n.writer.WriteByte(':')
	if err != nil {
		return err
	}

	for _, chunk := range data {
		_, err = n.writer.Write(chunk)
		if err != nil {
			return err
		}
	}

	_, err = n.writer.WriteString(",\n")
	if err != nil {
		return err
	}

	err = n.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}
