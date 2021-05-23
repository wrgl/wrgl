// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"
	"reflect"
	"strings"
)

func getFieldValue(s interface{}, prop string, createIfZero bool) (reflect.Value, error) {
	props := strings.Split(prop, ".")
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("interface isn't a struct")
	}
	for _, p := range props {
		name := p
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			v = v.Elem()
		}

		switch t.Kind() {
		case reflect.Struct:
			name = strings.ToUpper(string(p[0])) + p[1:]
			sf, ok := t.FieldByName(name)
			if !ok {
				return reflect.Value{}, fmt.Errorf(`field "%s" not found`, name)
			}
			v = v.FieldByName(name)
			t = sf.Type
			if v.IsZero() {
				if createIfZero {
					if t.Kind() == reflect.Ptr {
						v.Set(reflect.New(t.Elem()))
					} else {
						v.Set(reflect.New(t).Elem())
					}
				} else {
					return reflect.Value{}, fmt.Errorf(`field "%s" is zero`, name)
				}
			}
		case reflect.Map:
			if t.Key().Kind() != reflect.String {
				return reflect.Value{}, fmt.Errorf("map key must be a string")
			}
			t = t.Elem()
			key := reflect.ValueOf(name)
			e := v.MapIndex(key)
			if !e.IsValid() {
				if createIfZero {
					if t.Kind() == reflect.Ptr {
						e = reflect.New(t.Elem())
					} else {
						e = reflect.New(t).Elem()
					}
					v.SetMapIndex(key, e)
				} else {
					return reflect.Value{}, fmt.Errorf("key not found: %q", name)
				}
			}
			v = e
		default:
			return reflect.Value{}, fmt.Errorf("invalid kind %v", t.Kind())
		}
	}
	return v, nil
}

func GetWithDotNotation(s interface{}, prop string) (interface{}, error) {
	fv, err := getFieldValue(s, prop, false)
	if err != nil {
		return nil, err
	}
	return fv.Interface(), nil
}

func SetWithDotNotation(s interface{}, prop string, val interface{}) error {
	fv, err := getFieldValue(s, prop, true)
	if err != nil {
		return err
	}
	fv.Set(reflect.ValueOf(val))
	return nil
}
