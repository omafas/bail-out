package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/pkg/errors"
)

func (d *Decoder) readInteger(expectedTag Tag) (v int32, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(INTEGER); err != nil {
		return
	}

	if err = d.expectLength(4); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	v = int32(binary.BigEndian.Uint32(b[:4]))

	return
}

func (d *Decoder) readLongInteger(expectedTag Tag) (v int64, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(LONG_INTEGER); err != nil {
		return
	}

	if err = d.expectLength(8); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	v = int64(binary.BigEndian.Uint64(b[:]))

	return
}

func (d *Decoder) readEnum(expectedTag Tag) (v Enum, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(ENUMERATION); err != nil {
		return
	}

	if err = d.expectLength(4); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	v = Enum(binary.BigEndian.Uint32(b[:4]))

	return
}

func (d *Decoder) readBool(expectedTag Tag) (v bool, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(BOOLEAN); err != nil {
		return
	}

	if err = d.expectLength(8); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	for i := 0; i < 7; i++ {
		if b[i] != 0 {
			err = errors.Errorf("unexpected boolean value: %v", b)
			return
		}
	}

	switch b[7] {
	case 1:
		v = true
	case 0:
		v = false
	default:
		err = errors.Errorf("unexpected boolean value: %v", b)
	}

	return
}

func (d *Decoder) readByteSlice(expectedTag Tag, expectedType Type) (n int, v []byte, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(expectedType); err != nil {
		return
	}

	var l uint32
	if l, err = d.readLength(); err != nil {
		return
	}

	v = make([]byte, l)
	_, err = io.ReadFull(d.r, v)
	if err != nil {
		return
	}

	n = int(l) + 8

	// padding
	var b [8]byte
	if l%8 != 0 {
		_, err = io.ReadFull(d.r, b[:8-l%8])
		if err != nil {
			return
		}
		n += int(8 - l%8)
	}

	return
}

func (d *Decoder) readBytes(expectedTag Tag) (n int, v []byte, err error) {
	n, v, err = d.readByteSlice(expectedTag, BYTE_STRING)
	return
}

func (d *Decoder) readString(expectedTag Tag) (n int, v string, err error) {
	var b []byte
	n, b, err = d.readByteSlice(expectedTag, TEXT_STRING)
	v = string(b)
	return
}

func (d *Decoder) readTime(expectedTag Tag) (v time.Time, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(DATE_TIME); err != nil {
		return
	}

	if err = d.expectLength(8); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	v = time.Unix(int64(binary.BigEndian.Uint64(b[:])), 0)

	return
}

func (d *Decoder) readDuration(expectedTag Tag) (v time.Duration, err error) {
	if err = d.expectTag(expectedTag); err != nil {
		return
	}

	if err = d.expectType(INTERVAL); err != nil {
		return
	}

	if err = d.expectLength(4); err != nil {
		return
	}

	var b [8]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	v = time.Duration(binary.BigEndian.Uint32(b[:4])) * time.Second

	return
}
