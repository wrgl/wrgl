// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dotno

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"

	"github.com/spf13/cobra"
)

var errNotStringSlice = fmt.Errorf("VALUE_PATTERN should only be specified for options that accept multiple strings")

func presentableAsText(t reflect.Type) bool {
	return (t.Kind() == reflect.String || t.Implements(reflect.TypeOf((*fmt.Stringer)(nil)).Elem()) || t.Implements(reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()))
}

func marshalText(v reflect.Value) (s string, err error) {
	t := v.Type()
	if t.Kind() == reflect.String {
		s = v.String()
		return
	}
	if t.Implements(reflect.TypeOf((*fmt.Stringer)(nil)).Elem()) {
		s = v.Interface().(fmt.Stringer).String()
		return
	}
	if t.Implements(reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()) {
		b, err := v.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return
	}
	s = string(b)
	return
}

func FilterWithValuePattern(cmd *cobra.Command, v reflect.Value, valuePattern string) (idxMap map[int]struct{}, vals []string, err error) {
	fixedValue, err := cmd.Flags().GetBool("fixed-value")
	if err != nil {
		return
	}
	t := v.Type()
	if t.Kind() != reflect.Slice {
		err = errNotStringSlice
		return
	}
	switch t.Elem().Kind() {
	case reflect.Struct, reflect.Slice:
		err = errNotStringSlice
		return
	case reflect.Ptr:
		switch t.Elem().Elem().Kind() {
		case reflect.Struct, reflect.Slice:
			if !presentableAsText(t.Elem()) {
				err = errNotStringSlice
				return
			}
		}
	}
	var elems []string
	n := v.Len()
	for i := 0; i < n; i++ {
		s, err := marshalText(v.Index(i))
		if err != nil {
			return nil, nil, err
		}
		elems = append(elems, s)
	}
	idxMap = map[int]struct{}{}
	if fixedValue {
		for i, s := range elems {
			if s == valuePattern {
				idxMap[i] = struct{}{}
				vals = append(vals, s)
			}
		}
	} else {
		pat, err := regexp.Compile(valuePattern)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing VALUE_PATTERN: %v", err)
		}
		for i, s := range elems {
			if pat.MatchString(s) {
				idxMap[i] = struct{}{}
				vals = append(vals, s)
			}
		}
	}
	return
}

func OutputValues(cmd *cobra.Command, vals interface{}, lastOneOnly bool) (err error) {
	null, err := cmd.Flags().GetBool("null")
	if err != nil {
		return
	}
	v := reflect.ValueOf(vals)
	if v.Kind() == reflect.Slice {
		n := v.Len()
		if lastOneOnly {
			s, err := marshalText(v.Index(n - 1))
			if err != nil {
				return err
			}
			cmd.Print(s)
		} else {
			for i := 0; i < n; i++ {
				s, err := marshalText(v.Index(i))
				if err != nil {
					return err
				}
				cmd.Print(s)
				if i < n-1 {
					cmd.Print("\n")
				}
			}
		}
	} else {
		s, err := marshalText(v)
		if err != nil {
			return err
		}
		cmd.Print(s)
	}
	if null {
		cmd.Print("\x00")
	} else {
		cmd.Print("\n")
	}
	return nil
}
