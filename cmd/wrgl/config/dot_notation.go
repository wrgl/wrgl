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
	if prop == "" {
		return v, nil
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
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
					} else if t.Kind() == reflect.Map {
						v.Set(reflect.MakeMap(t))
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
			return reflect.Value{}, fmt.Errorf("unhandled kind %v", t.Kind())
		}
	}
	return v, nil
}

func getParentField(s interface{}, prop string) (parent reflect.Value, name string, err error) {
	props := strings.Split(prop, ".")
	n := len(props) - 1
	name = props[n]
	props = props[:n]
	parent, err = getFieldValue(s, strings.Join(props, "."), false)
	return
}

func unsetField(s interface{}, prop string, all bool) (err error) {
	parent, name, err := getParentField(s, prop)
	if err != nil {
		return
	}
	if parent.Type().Kind() == reflect.Ptr {
		parent = parent.Elem()
	}
	switch parent.Type().Kind() {
	case reflect.Struct:
		name = strings.ToUpper(string(name[0])) + name[1:]
		field := parent.FieldByName(name)
		if !all && field.Kind() == reflect.Slice && len(field.Interface().([]string)) > 1 {
			return fmt.Errorf("key contains multiple values")
		}
		field.Set(reflect.New(field.Type()).Elem())
	case reflect.Map:
		ft := parent.Type().Elem()
		key := reflect.ValueOf(name)
		if !all && ft.Kind() == reflect.Slice {
			field := parent.MapIndex(key)
			if len(field.Interface().([]string)) > 1 {
				return fmt.Errorf("key contains multiple values")
			}
		}
		parent.SetMapIndex(key, reflect.Value{})
	default:
		return fmt.Errorf("unhandled kind %v", parent.Type().Kind())
	}
	return nil
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
