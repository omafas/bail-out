package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"reflect"
	"time"
)

// Tag is a number that designates the specific Protocol Field or Object that the TTLV object represents
type Tag uint32

// Type is a byte containing a coded value that indicates the data type of the data object
type Type uint8

// Enum is KMIP Enumeration type
type Enum uint32

// DynamicDispatch is an interface for structure go set field value based on other field values
type DynamicDispatch interface {
	BuildFieldValue(name string) (interface{}, error)
}

var (
	typeOfTag      = reflect.TypeOf(Tag(0))
	typeOfEnum     = reflect.TypeOf(Enum(0))
	typeOfInt32    = reflect.TypeOf(int32(0))
	typeOfInt64    = reflect.TypeOf(int64(0))
	typeOfBool     = reflect.TypeOf(false)
	typeOfBytes    = reflect.TypeOf([]byte(nil))
	typeOfString   = reflect.TypeOf("")
	typeOfTime     = reflect.TypeOf(time.Time{})
	typeOfDuration = reflect.TypeOf(time.Duration(0))
)
