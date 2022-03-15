// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package slice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuplicatedString(t *testing.T) {
	for _, r := range []struct {
		Ssl []string
		R   string
	}{
		{[]string{"1", "2"}, ""},
		{[]string{}, ""},
		{[]string{"abc"}, ""},
		{[]string{"1", "1"}, "1"},
		{[]string{"abc", "def", "abc"}, "abc"},
	} {
		assert.Equal(t, r.R, DuplicatedString(r.Ssl))
	}
}

func TestStringNotInSubset(t *testing.T) {
	for _, r := range []struct {
		S1 []string
		S2 []string
		R  string
	}{
		{[]string{}, []string{"1", "2"}, ""},
		{[]string{}, []string{}, ""},
		{[]string{"1"}, []string{"1", "2"}, ""},
		{[]string{"1"}, []string{"1"}, ""},
		{[]string{"1", "3"}, []string{"1", "3"}, ""},
		{[]string{"3"}, []string{"1", "2"}, "3"},
		{[]string{"1"}, []string{}, "1"},
		{[]string{"2"}, []string{"1"}, "2"},
		{[]string{"1", "3"}, []string{"1", "2"}, "3"},
	} {
		assert.Equal(t, r.R, StringNotInSubset(r.S1, r.S2))
	}
}

func TestIndicesToValues(t *testing.T) {
	for _, r := range []struct {
		S []string
		I []uint32
		V []string
	}{
		{[]string{}, []uint32{}, []string{}},
		{[]string{"a", "b"}, []uint32{0}, []string{"a"}},
		{[]string{"c", "d", "e"}, []uint32{2, 1}, []string{"e", "d"}},
	} {
		assert.Equal(t, r.V, IndicesToValues(r.S, r.I))
	}
}

func TestKeyIndices(t *testing.T) {
	for _, r := range []struct {
		S []string
		K []string
		I []uint32
		E error
	}{
		{[]string{"a", "b"}, []string{"b"}, []uint32{1}, nil},
		{[]string{"a", "b"}, []string{}, nil, nil},
		{[]string{"a", "b"}, []string{"c"}, []uint32(nil), fmt.Errorf(`key "c" not found in string slice`)},
		{[]string{}, []string{}, nil, nil},
	} {
		i, e := KeyIndices(r.S, r.K)
		assert.Equal(t, r.I, i)
		assert.Equal(t, r.E, e)
	}
}

func TestStringSliceEqual(t *testing.T) {
	for _, r := range []struct {
		Sl1 []string
		Sl2 []string
		R   bool
	}{
		{[]string{"1"}, []string{"1"}, true},
		{[]string{"1", "2"}, []string{"1", "2"}, true},
		{[]string{}, []string{}, true},
		{[]string{"1"}, []string{"2"}, false},
		{[]string{"2", "1"}, []string{"2"}, false},
		{[]string{"1"}, []string{"1", "2"}, false},
		{[]string{}, []string{"2"}, false},
		{[]string{"1"}, []string{}, false},
	} {
		b := StringSliceEqual(r.Sl1, r.Sl2)
		assert.Equal(t, r.R, b)
	}
}

func TestCompareStringSlices(t *testing.T) {
	for i, c := range []struct {
		sl, oldSl                 []string
		unchanged, added, removed []string
	}{
		{
			nil, nil,
			nil, nil, nil,
		},
		{
			[]string{"a"}, []string{"a"},
			[]string{"a"}, nil, nil,
		},
		{
			[]string{"a", "b"}, []string{"a"},
			[]string{"a"}, []string{"b"}, nil,
		},
		{
			[]string{"a"}, []string{"a", "c"},
			[]string{"a"}, nil, []string{"c"},
		},
		{
			[]string{"d"}, []string{"e"},
			nil, []string{"d"}, []string{"e"},
		},
	} {
		unchanged, added, removed := CompareStringSlices(c.sl, c.oldSl)
		assert.Equal(t, c.unchanged, unchanged, "case %d", i)
		assert.Equal(t, c.added, added, "case %d", i)
		assert.Equal(t, c.removed, removed, "case %d", i)
	}
}

func TestInsertToSortedStringSlice(t *testing.T) {
	for i, c := range []struct {
		Strings  []string
		String   string
		Expected []string
	}{
		{nil, "a", []string{"a"}},
		{[]string{"b"}, "a", []string{"a", "b"}},
		{[]string{"a", "c"}, "b", []string{"a", "b", "c"}},
		{[]string{"a", "b"}, "d", []string{"a", "b", "d"}},
	} {
		assert.Equal(t, c.Expected, InsertToSortedStringSlice(c.Strings, c.String), "case %d", i)
	}
}

func TestInsertToSortedBytesSlice(t *testing.T) {
	for i, c := range []struct {
		Bytes    [][]byte
		Byte     []byte
		Expected [][]byte
	}{
		{nil, []byte("a"), [][]byte{[]byte("a")}},
		{[][]byte{[]byte("b")}, []byte("a"), [][]byte{[]byte("a"), []byte("b")}},
		{[][]byte{[]byte("a"), []byte("c")}, []byte("b"), [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{[][]byte{[]byte("a"), []byte("b")}, []byte("d"), [][]byte{[]byte("a"), []byte("b"), []byte("d")}},
	} {
		assert.Equal(t, c.Expected, InsertToSortedBytesSlice(c.Bytes, c.Byte), "case %d", i)
	}
}
