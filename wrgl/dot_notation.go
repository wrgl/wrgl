// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

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
		name := strings.ToUpper(string(p[0])) + p[1:]
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			v = v.Elem()
		}
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
