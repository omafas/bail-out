package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"reflect"

	"github.com/pkg/errors"
)

// Decoder implements KMIP protocol decoding
//
// Decoding works exactly the same way as encoding
// (see Encoder documentation), but the other way around.
type Decoder struct {
	r io.Reader
	s io.ByteScanner

	lastTag Tag
}

// NewDecoder builds Decoder which reads from r
//
// Buffering can be disabled by providing reader
// which implements io.ByteScanner
func NewDecoder(r io.Reader) *Decoder {
	d := &Decoder{
		r: r,
	}

	if s, ok := r.(io.ByteScanner); ok {
		d.s = s
	} else {
		br := bufio.NewReader(r)
		d.r = br
		d.s = br
	}

	return d
}

func (d *Decoder) internalReadTag() (t Tag, err error) {
	var b [3]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	t = Tag(binary.BigEndian.Uint32(append([]byte{0}, b[:]...)))

	return
}

func (d *Decoder) readTag() (t Tag, err error) {
	if d.lastTag != 0 {
		t, d.lastTag = d.lastTag, 0
		return
	}

	t, err = d.internalReadTag()
	return
}

func (d *Decoder) peekTag() (t Tag, err error) {
	if d.lastTag != 0 {
		return d.lastTag, nil
	}

	d.lastTag, err = d.internalReadTag()
	t = d.lastTag

	return
}

func (d *Decoder) expectTag(expected Tag) error {
	t, err := d.readTag()
	if err != nil {
		return err
	}

	if expected != t && expected != ANY_TAG {
		return errors.Errorf("expecting tag %x, but %x was encountered", expected, t)
	}

	return nil
}

func (d *Decoder) readType() (t Type, err error) {
	var b byte

	b, err = d.s.ReadByte()
	t = Type(b)

	return
}

func (d *Decoder) expectType(expected Type) error {
	t, err := d.readType()
	if err != nil {
		return err
	}

	if expected != t {
		return errors.Errorf("expecting type %d, but %d was encountered", expected, t)
	}

	return nil
}

func (d *Decoder) readLength() (l uint32, err error) {
	var b [4]byte

	_, err = io.ReadFull(d.r, b[:])
	if err != nil {
		return
	}

	l = binary.BigEndian.Uint32(b[:])

	return
}

func (d *Decoder) expectLength(expected uint32) error {
	l, err := d.readLength()
	if err != nil {
		return err
	}

	if expected != l {
		return errors.Errorf("expecting length %d, but %d was encountered", expected, l)
	}

	return nil
}

// Decode structure from the reader into v
func (d *Decoder) Decode(v interface{}) error {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return errors.New("invalid value")
	}
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return errors.New("invalid pointer value")
	}
	if !rv.CanSet() {
		return errors.New("unsettable value")
	}

	structDesc, err := getStructDesc(rv.Type())
	if err != nil {
		return err
	}

	_, err = d.decode(rv, structDesc)
	return err
}

func (d *Decoder) decodeValue(f field, t reflect.Type, ff reflect.Value) (n int, v interface{}, err error) {
	if f.skip {
		if err = d.expectTag(f.tag); err != nil {
			return
		}

		if _, err = d.readType(); err != nil {
			return
		}

		var l uint32
		if l, err = d.readLength(); err != nil {
			return
		}

		n = 8

		if l%8 != 0 {
			l += 8 - l%8
		}

		_, err = io.CopyN(ioutil.Discard, d.r, int64(l))
		n += int(l)

		return
	}

	switch f.typ {
	case INTEGER:
		v, err = d.readInteger(f.tag)
		n = 16
	case LONG_INTEGER:
		v, err = d.readLongInteger(f.tag)
		n = 16
	case ENUMERATION:
		v, err = d.readEnum(f.tag)
		n = 16
	case BOOLEAN:
		v, err = d.readBool(f.tag)
		n = 16
	case DATE_TIME:
		v, err = d.readTime(f.tag)
		n = 16
	case INTERVAL:
		v, err = d.readDuration(f.tag)
		n = 16
	case BYTE_STRING:
		n, v, err = d.readBytes(f.tag)
	case TEXT_STRING:
		n, v, err = d.readString(f.tag)
	case STRUCTURE:
		var (
			sD *structDesc
			vv reflect.Value
		)

		if f.dynamic {
			dD, ok := ff.Addr().Interface().(DynamicDispatch)
			if !ok {
				err = errors.New("field is dynamic, but DynamicDispatch is not implemented")
				return
			}

			var val interface{}

			val, err = dD.BuildFieldValue(f.name)
			if err != nil {
				return
			}

			vv = reflect.ValueOf(val)

			switch vv.Type() {
			case typeOfInt32:
				f.typ = INTEGER
				return d.decodeValue(f, t, ff)
			case typeOfInt64:
				f.typ = LONG_INTEGER
				return d.decodeValue(f, t, ff)
			case typeOfEnum:
				f.typ = ENUMERATION
				return d.decodeValue(f, t, ff)
			case typeOfBool:
				f.typ = BOOLEAN
				return d.decodeValue(f, t, ff)
			case typeOfBytes:
				f.typ = BYTE_STRING
				return d.decodeValue(f, t, ff)
			case typeOfString:
				f.typ = TEXT_STRING
				return d.decodeValue(f, t, ff)
			case typeOfTime:
				f.typ = DATE_TIME
				return d.decodeValue(f, t, ff)
			}

			sD, err = getStructDesc(vv.Type().Elem())
			if err != nil {
				return
			}
		} else {

			sD, err = getStructDesc(t)
			if err != nil {
				return
			}

			vv = reflect.New(t)
		}

		sD.tag = f.tag
		n, err = d.decode(vv, sD)

		v = vv.Elem().Interface()
	default:
		panic("unsupported type")
	}

	return
}

func (d *Decoder) decode(rv reflect.Value, structD *structDesc) (n int, err error) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if err = d.expectTag(structD.tag); err != nil {
		return
	}

	if err = d.expectType(STRUCTURE); err != nil {
		return
	}

	n += 4

	var expectedLen, actualLen uint32
	if expectedLen, err = d.readLength(); err != nil {
		return
	}

	n += 4

	// initialize wrapped decoder with limited reader
	dd := NewDecoder(io.LimitReader(d.r, int64(expectedLen)))

	for _, f := range structD.fields {
		var tag Tag
		tag, err = dd.peekTag()

		if err == io.EOF && !f.required {
			err = nil
			continue
		}

		if err != nil {
			err = errors.Wrapf(err, "error reading field %v", f.name)
			return
		}

		if !f.required && tag != f.tag && f.tag != ANY_TAG {
			continue
		}

		var (
			nn int
			v  interface{}
		)

		ff := rv.FieldByIndex(f.idx)

		if f.sliceof {
			ff.Set(reflect.MakeSlice(ff.Type(), 0, 0))

			for {
				nn, v, err = dd.decodeValue(f, ff.Type().Elem(), rv)
				if err != nil {
					err = errors.Wrapf(err, "error reading field %v", f.name)
					return
				}

				n += nn
				actualLen += uint32(nn)

				if !f.skip {
					ff.Set(reflect.Append(ff, reflect.ValueOf(v)))
				}

				if actualLen >= expectedLen {
					break
				}

				tag, err = dd.peekTag()
				if err != nil {
					return
				}

				if tag != f.tag {
					break
				}
			}
		} else {
			nn, v, err = dd.decodeValue(f, ff.Type(), rv)
			if err != nil {
				err = errors.Wrapf(err, "error reading field %v", f.name)
				return
			}

			n += nn
			actualLen += uint32(nn)

			if !f.skip {
				ff.Set(reflect.ValueOf(v))
			}
		}
	}

	if actualLen != expectedLen {
		err = errors.Errorf("error reading structure expected %d != actual %d", expectedLen, actualLen)
	}

	return
}
