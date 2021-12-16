// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dotno

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

func FilterWithValuePattern(cmd *cobra.Command, v reflect.Value, valuePattern string) (idxMap map[int]struct{}, vals []string, err error) {
	fixedValue, err := cmd.Flags().GetBool("fixed-value")
	if err != nil {
		return
	}
	if v.Kind() != reflect.Slice {
		err = fmt.Errorf("VALUE_PATTERN should only be specified for options that accept multiple strings")
		return
	}
	sl, ok := ToTextSlice(v.Interface())
	if !ok {
		err = fmt.Errorf("type %v does not implement fmt.Stringer", v.Type())
		return
	}
	idxMap = map[int]struct{}{}
	n := sl.Len()
	if fixedValue {
		for i := 0; i < n; i++ {
			s, err := sl.Get(i)
			if err != nil {
				return nil, nil, err
			}
			if s == valuePattern {
				idxMap[i] = struct{}{}
				vals = append(vals, s)
			}
		}
	} else {
		pat, err := regexp.Compile(valuePattern)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid VALUE_PATTERN: %v", err)
		}
		for i := 0; i < n; i++ {
			s, err := sl.Get(i)
			if err != nil {
				return nil, nil, err
			}
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
	if sl, ok := ToTextSlice(vals); ok && sl.Len() > 0 {
		if lastOneOnly {
			s, err := sl.Get(sl.Len() - 1)
			if err != nil {
				return err
			}
			if null {
				cmd.Printf("%s\x00", s)
			} else {
				cmd.Printf("%s\n", s)
			}
		} else {
			strs, err := sl.ToStringSlice()
			if err != nil {
				return err
			}
			if null {
				cmd.Print(strings.Join(strs, "\x00"), "\x00")
			} else {
				cmd.Println(strings.Join(strs, "\n"))
			}
		}
	} else if v, ok := vals.(*bool); ok {
		if null {
			cmd.Printf("%+v\x00", *v)
		} else {
			cmd.Printf("%+v\n", *v)
		}
	} else {
		if null {
			cmd.Printf("%+v\x00", vals)
		} else {
			cmd.Printf("%+v\n", vals)
		}
	}
	return nil
}
