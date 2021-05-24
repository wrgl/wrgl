// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type myType struct {
	Alpha string
	Beta  struct {
		Gamma int
	}
	Delta *struct {
		Nu struct {
			Xi bool
		}
	}
	Omega map[string]*struct {
		Epsilon float64
	}
	Eta  []string
	Zeta map[string][]string
}

func TestSetWithDotNotation(t *testing.T) {
	o := &myType{
		Alpha: "abc",
		Beta: struct {
			Gamma int
		}{30},
		Omega: map[string]*struct {
			Epsilon float64
		}{"main": {1.2}},
	}
	v, err := GetWithDotNotation(o, "alpha")
	require.NoError(t, err)
	assert.Equal(t, "abc", v.(string))
	v, err = GetWithDotNotation(o, "beta.gamma")
	require.NoError(t, err)
	assert.Equal(t, 30, v.(int))

	err = SetWithDotNotation(o, "alpha", "def")
	require.NoError(t, err)
	assert.Equal(t, "def", o.Alpha)
	err = SetWithDotNotation(o, "beta.gamma", 10)
	require.NoError(t, err)
	assert.Equal(t, 10, o.Beta.Gamma)

	_, err = GetWithDotNotation(o, "delta.nu.xi")
	assert.Equal(t, `field "Delta" is zero`, err.Error())
	err = SetWithDotNotation(o, "delta.nu.xi", true)
	require.NoError(t, err)
	assert.True(t, o.Delta.Nu.Xi)

	// dealing with map
	v, err = GetWithDotNotation(o, "omega.main.epsilon")
	require.NoError(t, err)
	assert.Equal(t, 1.2, v.(float64))
	_, err = GetWithDotNotation(o, "omega.zeta.epsilon")
	assert.Equal(t, "key not found: \"zeta\"", err.Error())
	require.NoError(t, SetWithDotNotation(o, "omega.zeta.epsilon", 2.4))
	v, err = GetWithDotNotation(o, "omega.zeta.epsilon")
	require.NoError(t, err)
	assert.Equal(t, 2.4, v.(float64))
}

func TestUnsetField(t *testing.T) {
	for _, c := range []struct {
		name   string
		obj    *myType
		key    string
		all    bool
		result *myType
		errMsg string
	}{
		{
			name: "unset a shallow key",
			obj: &myType{
				Alpha: "abc",
				Beta: struct {
					Gamma int
				}{30},
			},
			key: "alpha",
			result: &myType{
				Beta: struct {
					Gamma int
				}{30},
			},
		},

		{
			name: "unset a struct",
			obj: &myType{
				Alpha: "abc",
				Beta: struct {
					Gamma int
				}{30},
			},
			key: "beta",
			result: &myType{
				Alpha: "abc",
			},
		},

		{
			name: "unset a nested field",
			obj: &myType{
				Alpha: "abc",
				Beta: struct {
					Gamma int
				}{30},
			},
			key: "beta.gamma",
			result: &myType{
				Alpha: "abc",
				Beta: struct {
					Gamma int
				}{},
			},
		},
		{
			name: "unset a map",
			obj: &myType{
				Alpha: "abc",
				Omega: map[string]*struct{ Epsilon float64 }{
					"main": {2.4},
				},
			},
			key: "omega",
			result: &myType{
				Alpha: "abc",
			},
		},
		{
			name: "unset a map key",
			obj: &myType{
				Alpha: "abc",
				Omega: map[string]*struct{ Epsilon float64 }{
					"main": {2.4},
					"aux":  {2.3},
				},
			},
			key: "omega.main",
			result: &myType{
				Alpha: "abc",
				Omega: map[string]*struct{ Epsilon float64 }{
					"aux": {2.3},
				},
			},
		},
		{
			name: "unset a slice",
			obj: &myType{
				Alpha: "abc",
				Eta:   []string{"a"},
			},
			key: "eta",
			result: &myType{
				Alpha: "abc",
			},
		},
		{
			name: "unset a slice with multiple values",
			obj: &myType{
				Alpha: "abc",
				Eta:   []string{"a", "b"},
			},
			key:    "eta",
			errMsg: "key contains multiple values",
		},
		{
			name: "unset a slice even with multiple values",
			obj: &myType{
				Alpha: "abc",
				Eta:   []string{"a", "b"},
			},
			key: "eta",
			all: true,
			result: &myType{
				Alpha: "abc",
			},
		},
		{
			name: "unset a slice within map",
			obj: &myType{
				Alpha: "abc",
				Zeta:  map[string][]string{"a": {"b"}},
			},
			key: "zeta.a",
			result: &myType{
				Alpha: "abc",
				Zeta:  map[string][]string{},
			},
		},
		{
			name: "unset a slice within map with multiple values",
			obj: &myType{
				Alpha: "abc",
				Zeta: map[string][]string{
					"c": {"a", "b"},
					"d": {"e", "f"},
				},
			},
			key:    "zeta.c",
			errMsg: "key contains multiple values",
		},
		{
			name: "unset a slice within map even with multiple values",
			obj: &myType{
				Alpha: "abc",
				Zeta: map[string][]string{
					"c": {"a", "b"},
					"d": {"e", "f"},
				},
			},
			key: "zeta.c",
			all: true,
			result: &myType{
				Alpha: "abc",
				Zeta: map[string][]string{
					"d": {"e", "f"},
				},
			},
		},
	} {
		err := unsetField(c.obj, c.key, c.all)
		if c.errMsg != "" {
			assert.Equal(t, c.errMsg, err.Error(), c.name)
		} else {
			require.NoError(t, err, c.name)
			assert.Equal(t, c.result, c.obj, c.name)
		}
	}
}
