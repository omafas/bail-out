package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type field struct {
	name     string
	idx      []int
	tag      Tag
	typ      Type
	required bool
	sliceof  bool
	skip     bool
	dynamic  bool
}

type structDesc struct {
	tag Tag

	fields []field
}

func parseTag(tag string) (name, opt string) {
	parts := strings.SplitN(tag, ",", 2)
	name = parts[0]
	if len(parts) > 1 {
		opt = parts[1]
	}

	return
}

func guessType(ft reflect.Type, f *field) error {
	switch ft {
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
	default:
		if ft.Kind() == reflect.Struct {
			f.typ = STRUCTURE
		} else if ft.Kind() == reflect.Interface {
			f.typ = STRUCTURE
			f.dynamic = true
		} else {
			return errors.Errorf("unsupported type %s", ft.String())
		}
	}

	return nil
}

func getStructDesc(tt reflect.Type) (*structDesc, error) {
	res := &structDesc{}

	for i := 0; i < tt.NumField(); i++ {
		ff := tt.Field(i)

		name, opt := parseTag(ff.Tag.Get("kmip"))

		if ff.Type == typeOfTag {
			var ok bool
			if res.tag, ok = tagMap[name]; !ok {
				return nil, errors.Errorf("unknown tag %v for struct tag", name)
			}
			continue
		}

		if name == "" || ff.PkgPath != "" {
			continue
		}

		f := field{
			name: ff.Name,
			idx:  ff.Index,
		}

		var ok bool
		if f.tag, ok = tagMap[name]; !ok {
			return nil, errors.Errorf("unknown tag %v for field %v", name, ff.Name)
		}

		f.required = strings.Contains(opt, "required")
		f.skip = strings.Contains(opt, "skip")

		ft := ff.Type
		if ft.Kind() == reflect.Slice && ft != typeOfBytes {
			f.sliceof = true
			ft = ft.Elem()
		}

		if err := guessType(ft, &f); err != nil {
			return nil, errors.WithMessagef(err, "error processing field %v", ff.Name)
		}

		res.fields = append(res.fields, f)
	}

	return res, nil
}
