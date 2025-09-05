package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"encoding/binary"
	"time"
)

func (e *Encoder) writeInteger(t Tag, v int32) (err error) {
	err = e.writeTagTypeLength(t, INTEGER, 4)
	if err != nil {
		return
	}

	var b [8]byte

	binary.BigEndian.PutUint32(b[:4], uint32(v))

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) writeLongInteger(t Tag, v int64) (err error) {
	err = e.writeTagTypeLength(t, LONG_INTEGER, 8)
	if err != nil {
		return
	}

	var b [8]byte

	binary.BigEndian.PutUint64(b[:], uint64(v))

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) writeEnum(t Tag, v Enum) (err error) {
	err = e.writeTagTypeLength(t, ENUMERATION, 4)
	if err != nil {
		return
	}

	var b [8]byte

	binary.BigEndian.PutUint32(b[:4], uint32(v))

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) writeBool(t Tag, v bool) (err error) {
	err = e.writeTagTypeLength(t, BOOLEAN, 8)
	if err != nil {
		return
	}

	var b [8]byte

	if v {
		b[7] = 1
	}

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) writeByteSlice(t Tag, typ Type, b []byte) (err error) {
	err = e.writeTagTypeLength(t, typ, uint32(len(b)))
	if err != nil {
		return
	}

	_, err = e.w.Write(b)
	if err != nil {
		return
	}

	if len(b)%8 != 0 {
		var pad [8]byte

		_, err = e.w.Write(pad[:8-len(b)%8])
	}

	return
}

func (e *Encoder) writeBytes(t Tag, b []byte) error {
	return e.writeByteSlice(t, BYTE_STRING, b)
}

func (e *Encoder) writeString(t Tag, s string) error {
	return e.writeByteSlice(t, TEXT_STRING, []byte(s))
}

func (e *Encoder) writeTime(t Tag, v time.Time) (err error) {
	err = e.writeTagTypeLength(t, DATE_TIME, 8)
	if err != nil {
		return
	}

	var b [8]byte

	binary.BigEndian.PutUint64(b[:], uint64(v.Unix()))

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) writeDuration(t Tag, v time.Duration) (err error) {
	err = e.writeTagTypeLength(t, INTERVAL, 4)
	if err != nil {
		return
	}

	var b [8]byte

	binary.BigEndian.PutUint32(b[:4], uint32(v/time.Second))

	_, err = e.w.Write(b[:])
	return
}
