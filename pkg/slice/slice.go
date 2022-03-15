// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package slice

import (
	"fmt"
	"sort"
)

func DuplicatedString(s []string) string {
	m := map[string]string{}
	for _, k := range s {
		if _, ok := m[k]; ok {
			return k
		}
		m[k] = k
	}
	return ""
}

func StringNotInSubset(s1, s2 []string) string {
	m := map[string]string{}
	for _, k := range s2 {
		m[k] = k
	}
	for _, k := range s1 {
		if _, ok := m[k]; !ok {
			return k
		}
	}
	return ""
}

func IndicesToValues(vals []string, keys []uint32) []string {
	res := make([]string, 0, len(keys))
	for _, k := range keys {
		res = append(res, vals[k])
	}
	return res
}

func KeyIndices(columns, keys []string) (res []uint32, err error) {
	for _, k := range keys {
		found := false
		for i, c := range columns {
			if c == k {
				res = append(res, uint32(i))
				found = true
				continue
			}
		}
		if !found {
			return nil, fmt.Errorf(`key "%s" not found in string slice`, k)
		}
	}
	return res, nil
}

func StringSliceEqual(sl1, sl2 []string) bool {
	if len(sl1) != len(sl2) {
		return false
	}
	for i, v := range sl1 {
		if v != sl2[i] {
			return false
		}
	}
	return true
}

func StringSliceContains(sl []string, s string) bool {
	for _, v := range sl {
		if v == s {
			return true
		}
	}
	return false
}

func CompareStringSlices(slice, oldSlice []string) (unchanged, added, removed []string) {
	m := map[string]struct{}{}
	for _, col := range slice {
		m[col] = struct{}{}
	}
	oldM := map[string]struct{}{}
	for _, col := range oldSlice {
		oldM[col] = struct{}{}
	}
	for _, col := range slice {
		if _, ok := oldM[col]; !ok {
			added = append(added, col)
		} else {
			unchanged = append(unchanged, col)
		}
	}
	for _, col := range oldSlice {
		if _, ok := m[col]; !ok {
			removed = append(removed, col)
		}
	}
	return
}

func InsertToSortedStringSlice(sl []string, s string) []string {
	i := sort.SearchStrings(sl, s)
	if i == 0 {
		sl = append([]string{s}, sl...)
	} else {
		sl = append(sl[:i], sl[i-1:]...)
		sl[i] = s
	}
	return sl
}

func InsertToSortedBytesSlice(sl [][]byte, b []byte) [][]byte {
	s := string(b)
	i := sort.Search(len(sl), func(i int) bool {
		return string(sl[i]) >= s
	})
	if i == 0 {
		sl = append([][]byte{b}, sl...)
	} else {
		sl = append(sl[:i], sl[i-1:]...)
		sl[i] = b
	}
	return sl
}
