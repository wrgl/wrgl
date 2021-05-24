// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextSliceFromStringSlice(t *testing.T) {
	strs := []string{"a", "b", "c"}
	sl, ok := ToTextSlice(strs)
	assert.True(t, ok)
	assert.Equal(t, 3, sl.Len())
	for i, s := range strs {
		o, err := sl.Get(i)
		require.NoError(t, err)
		assert.Equal(t, s, o)
	}

	err := sl.Set(1, "d")
	require.NoError(t, err)
	s, err := sl.Get(1)
	require.NoError(t, err)
	assert.Equal(t, "d", s)

	err = sl.Append("e", "f")
	require.NoError(t, err)

	strs, err = sl.ToStringSlice()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "d", "c", "e", "f"}, strs)
	assert.Equal(t, []string{"a", "d", "c", "e", "f"}, sl.Value.Interface())

	sl, err = TextSliceFromStrSlice(reflect.TypeOf([]string{}), []string{"x", "y", "z"})
	require.NoError(t, err)
	assert.Equal(t, []string{"x", "y", "z"}, sl.Value.Interface())
}

type myText struct {
	s string
}

func (o *myText) MarshalText() ([]byte, error) {
	return []byte(o.s), nil
}

func (o *myText) UnmarshalText(b []byte) error {
	o.s = string(b)
	return nil
}

func TestTextSlice(t *testing.T) {
	objs := []*myText{
		{"a"}, {"b"}, {"c"},
	}
	sl, ok := ToTextSlice(objs)
	assert.True(t, ok)
	assert.Equal(t, 3, sl.Len())
	for i, s := range objs {
		o, err := sl.Get(i)
		require.NoError(t, err)
		assert.Equal(t, s.s, o)
	}

	err := sl.Set(1, "d")
	require.NoError(t, err)
	s, err := sl.Get(1)
	require.NoError(t, err)
	assert.Equal(t, "d", s)

	err = sl.Append("e", "f")
	require.NoError(t, err)

	strs, err := sl.ToStringSlice()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "d", "c", "e", "f"}, strs)
	assert.Equal(t, []*myText{
		{"a"}, {"d"}, {"c"}, {"e"}, {"f"},
	}, sl.Value.Interface())

	sl, err = TextSliceFromStrSlice(reflect.TypeOf(objs), []string{"x", "y", "z"})
	require.NoError(t, err)
	assert.Equal(t, []*myText{{"x"}, {"y"}, {"z"}}, sl.Value.Interface())
}
