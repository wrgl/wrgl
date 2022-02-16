// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dotno

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func parseSliceIndex(v reflect.Value, index string) (int, error) {
	ind, err := strconv.Atoi(index)
	if err != nil {
		err = fmt.Errorf("error parsing key %q as number: %v", index, err)
		return 0, err
	}
	if n := v.Len(); ind >= n {
		return 0, fmt.Errorf("index out of range: %d >= %d", ind, n)
	}
	return ind, nil
}

// structField try all variants of a name on struct and return the first matching struct field
func structField(structType reflect.Type, name string) (sf *reflect.StructField, err error) {
	nameLower := strings.ToLower(name)
	n := structType.NumField()
	for i := 0; i < n; i++ {
		field := structType.Field(i)
		r, _ := utf8.DecodeRuneInString(field.Name)
		// only consider exported fields
		if unicode.IsLower(r) {
			continue
		}
		if strings.ToLower(field.Name) == nameLower {
			return &field, nil
		}
	}
	return nil, fmt.Errorf("field %q not found", name)
}

func GetFieldValue(s interface{}, prop string, createIfZero bool) (v reflect.Value, err error) {
	props := strings.Split(prop, ".")
	v = reflect.ValueOf(s)
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
			sf, err := structField(t, name)
			if err != nil {
				return reflect.Value{}, err
			}
			v = v.FieldByName(sf.Name)
			t = sf.Type
			if v.IsZero() {
				if !createIfZero {
					return reflect.Value{}, fmt.Errorf(`field "%s" is zero`, sf.Name)
				}
				if t.Kind() == reflect.Ptr {
					v.Set(reflect.New(t.Elem()))
				} else if t.Kind() == reflect.Map {
					v.Set(reflect.MakeMap(t))
				} else {
					v.Set(reflect.New(t).Elem())
				}
			}
		case reflect.Map:
			if t.Key().Kind() != reflect.String {
				err = fmt.Errorf("map key must be a string")
				return
			}
			t = t.Elem()
			key := reflect.ValueOf(name)
			e := v.MapIndex(key)
			if !e.IsValid() {
				if !createIfZero {
					err = fmt.Errorf("key not found: %q", name)
					return
				}
				if t.Kind() == reflect.Ptr {
					e = reflect.New(t.Elem())
				} else {
					e = reflect.New(t).Elem()
				}
				v.SetMapIndex(key, e)
			}
			v = e
		case reflect.Slice:
			ind, err := parseSliceIndex(v, name)
			if err != nil {
				return v, err
			}
			t = t.Elem()
			v = v.Index(ind)
		default:
			return reflect.Value{}, fmt.Errorf("unhandled kind %v", t.Kind())
		}
	}
	return v, nil
}

func GetParentField(s interface{}, prop string) (parent reflect.Value, name string, err error) {
	props := strings.Split(prop, ".")
	n := len(props) - 1
	name = props[n]
	props = props[:n]
	parent, err = GetFieldValue(s, strings.Join(props, "."), false)
	return
}

func UnsetField(s interface{}, prop string, all bool) (err error) {
	parent, name, err := GetParentField(s, prop)
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
		if !all && field.Kind() == reflect.Slice && field.Len() > 1 {
			return fmt.Errorf("key contains multiple values")
		}
		field.Set(reflect.Zero(field.Type()))
	case reflect.Map:
		ft := parent.Type().Elem()
		key := reflect.ValueOf(name)
		field := parent.MapIndex(key)
		if !all && ft.Kind() == reflect.Slice && field.Len() > 1 {
			return fmt.Errorf("key contains multiple values")
		}
		parent.SetMapIndex(key, reflect.Value{})
	case reflect.Slice:
		ind, err := parseSliceIndex(parent, name)
		if err != nil {
			return err
		}
		field := parent.Index(ind)
		parentType := parent.Type()
		if !all && parentType.Elem().Kind() == reflect.Slice && field.Len() > 1 {
			return fmt.Errorf("key contains multiple values")
		}
		n := parent.Len()
		v := reflect.MakeSlice(parentType, n-1, n-1)
		reflect.Copy(v, parent.Slice(0, ind))
		if ind < n-1 {
			reflect.Copy(v.Slice(ind, n-1), parent.Slice(ind+1, n))
		}
		parent.Set(v)
	default:
		return fmt.Errorf("unhandled kind %v", parent.Type().Kind())
	}
	return nil
}

func SetValue(v reflect.Value, val string) error {
	if v.Kind() == reflect.Ptr && v.IsZero() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	if v.Type().Implements(reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()) {
		o := v.Interface().(encoding.TextUnmarshaler)
		return o.UnmarshalText([]byte(val))
	}
	switch v.Kind() {
	case reflect.String:
		v.Set(reflect.ValueOf(val).Convert(v.Type()))
	case reflect.Ptr:
		switch v.Type().Elem().Kind() {
		case reflect.String:
			return SetValue(v.Elem(), val)
		default:
			if err := json.Unmarshal([]byte(val), v.Interface()); err != nil {
				return err
			}
		}
	default:
		o := reflect.New(v.Type())
		if err := SetValue(o, val); err != nil {
			return err
		}
		v.Set(o.Elem())
	}
	return nil
}

func AppendSlice(sl reflect.Value, values ...string) (err error) {
	t := sl.Type()
	if t.Kind() != reflect.Ptr && t.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("can only append to pointer of slice")
	}
	elems := sl.Elem()
	n := elems.Len()
	m := len(values)
	v := reflect.MakeSlice(t.Elem(), n+m, n+m)
	reflect.Copy(v, elems)
	for i, s := range values {
		if err := SetValue(v.Index(n+i), s); err != nil {
			return err
		}
	}
	elems.Set(v)
	return nil
}
