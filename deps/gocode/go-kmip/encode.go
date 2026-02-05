package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

// Encoder implements encoding to TTLV KMIP protocol format
//
// All core types are supported:
//
//  * Integer (int32)
//  * Long Integer (int64)
//  * Enumeration (Enum)
//  * Boolean (bool)
//  * Bytes ([]byte)
//  * String (string)
//  * Timestamp (time.Time)
//  * Interval (time.Duration)
//
// Encoder processes Go structure, analyzing field tags and parsing out
// `kmip` Go struct tags, e.g.:
//	  Value string `kmip:"TAG_NAME,required"`
// KMIP TAG_NAME is looked up to find tag value, Go type is translated to
// respective KMIP core type (see above), length is automatically calculated.
//
// Fields with zero value which are not required are skipped while encoding.
type Encoder struct {
	w io.Writer
}

// NewEncoder builds encoder writing to w
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: w,
	}
}

func (e *Encoder) writeTagTypeLength(t Tag, typ Type, l uint32) (err error) {
	var (
		b  [8]byte
		tt [4]byte
	)

	binary.BigEndian.PutUint32(tt[:], uint32(t))

	copy(b[:3], tt[1:])
	b[3] = byte(typ)
	binary.BigEndian.PutUint32(b[4:], l)

	_, err = e.w.Write(b[:])
	return
}

func (e *Encoder) Encode(v interface{}) (err error) {
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

	var structDesc *structDesc
	structDesc, err = getStructDesc(rv.Type())
	if err != nil {
		return err
	}

	err = e.encode(rv, structDesc)
	return
}

func (e *Encoder) encodeValue(f field, rt reflect.Type, rv reflect.Value) (err error) {
	if rv.Kind() == reflect.Interface && !rv.IsNil() {
		rv = rv.Elem()
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		rt = rv.Type()

		switch rt {
		case typeOfInt32:
			f.typ = INTEGER
		case typeOfInt64:
			f.typ = LONG_INTEGER
		case typeOfEnum:
			f.typ = ENUMERATION
		case typeOfBool:
			f.typ = BOOLEAN
		case typeOfBytes:
			f.typ = BYTE_STRING
		case typeOfString:
			f.typ = TEXT_STRING
		case typeOfTime:
			f.typ = DATE_TIME
		case typeOfDuration:
			f.typ = INTERVAL
		}
	}

	switch f.typ {
	case INTEGER:
		err = e.writeInteger(f.tag, int32(rv.Int()))
	case LONG_INTEGER:
		err = e.writeLongInteger(f.tag, rv.Int())
	case ENUMERATION:
		err = e.writeEnum(f.tag, Enum(rv.Uint()))
	case BOOLEAN:
		err = e.writeBool(f.tag, rv.Bool())
	case DATE_TIME:
		err = e.writeTime(f.tag, rv.Interface().(time.Time))
	case INTERVAL:
		err = e.writeDuration(f.tag, rv.Interface().(time.Duration))
	case BYTE_STRING:
		err = e.writeBytes(f.tag, rv.Bytes())
	case TEXT_STRING:
		err = e.writeString(f.tag, rv.String())
	case STRUCTURE:
		var structDesc *structDesc

		structDesc, err = getStructDesc(rt)
		if err != nil {
			return
		}

		structDesc.tag = f.tag

		err = e.encode(rv, structDesc)
	default:
		err = errors.Errorf("unsupported type for encode, field %v", f.name)
	}

	return
}

func isZeroValue(rv reflect.Value) (bool, error) {
	switch rv.Kind() {
	case reflect.Array, reflect.Slice, reflect.String:
		return rv.Len() == 0, nil
	case reflect.Bool:
		return !rv.Bool(), nil
	case reflect.Int32, reflect.Int64:
		return rv.Int() == 0, nil
	case reflect.Uint32:
		return rv.Uint() == 0, nil
	case reflect.Interface, reflect.Ptr:
		return rv.IsNil(), nil
	case reflect.Struct:
		if rv.Type() == typeOfTime {
			return rv.Interface().(time.Time).IsZero(), nil
		}

		sD, err := getStructDesc(rv.Type())
		if err != nil {
			return false, err
		}

		for _, f := range sD.fields {
			isZero, err := isZeroValue(rv.FieldByIndex(f.idx))
			if err != nil {
				return false, err
			}
			if !isZero {
				return false, nil
			}
		}

		return true, nil
	default:
		return false, errors.Errorf("unsupported value for isZeroValue: %v", rv.Kind().String())
	}
}

func (e *Encoder) encode(rv reflect.Value, sd *structDesc) (err error) {
	// build new encoder to encode into temp buf (to know the length)
	var buf bytes.Buffer

	ee := NewEncoder(&buf)

	for _, f := range sd.fields {
		if f.tag == ANY_TAG || f.skip {
			continue
		}

		ff := rv.FieldByIndex(f.idx)

		if f.sliceof {
			for i := 0; i < ff.Len(); i++ {
				err = ee.encodeValue(f, ff.Type().Elem(), ff.Index(i))
				if err != nil {
					return
				}
			}
		} else {
			if !f.required {
				var isZero bool
				isZero, err = isZeroValue(ff)
				if err != nil {
					return
				}

				isVersion := f.name == "Minor" || f.name == "Major"
				if isZero && !isVersion {
					continue
				}
			}

			err = ee.encodeValue(f, ff.Type(), ff)
			if err != nil {
				return
			}
		}
	}

	err = e.writeTagTypeLength(sd.tag, STRUCTURE, uint32(buf.Len()))
	if err != nil {
		return
	}

	_, err = io.Copy(e.w, &buf)
	return
}
