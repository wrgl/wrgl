// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetWithDotNotation(t *testing.T) {
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
	}
	i := &myType{
		Alpha: "abc",
		Beta: struct {
			Gamma int
		}{30},
		Omega: map[string]*struct {
			Epsilon float64
		}{"main": {1.2}},
	}
	v, err := GetWithDotNotation(i, "alpha")
	require.NoError(t, err)
	assert.Equal(t, "abc", v.(string))
	v, err = GetWithDotNotation(i, "beta.gamma")
	require.NoError(t, err)
	assert.Equal(t, 30, v.(int))

	err = SetWithDotNotation(i, "alpha", "def")
	require.NoError(t, err)
	assert.Equal(t, "def", i.Alpha)
	err = SetWithDotNotation(i, "beta.gamma", 10)
	require.NoError(t, err)
	assert.Equal(t, 10, i.Beta.Gamma)

	_, err = GetWithDotNotation(i, "delta.nu.xi")
	assert.Equal(t, `field "Delta" is zero`, err.Error())
	err = SetWithDotNotation(i, "delta.nu.xi", true)
	require.NoError(t, err)
	assert.True(t, i.Delta.Nu.Xi)

	// dealing with map
	v, err = GetWithDotNotation(i, "omega.main.epsilon")
	require.NoError(t, err)
	assert.Equal(t, 1.2, v.(float64))
	_, err = GetWithDotNotation(i, "omega.zeta.epsilon")
	assert.Equal(t, "key not found: \"zeta\"", err.Error())
	require.NoError(t, SetWithDotNotation(i, "omega.zeta.epsilon", 2.4))
	v, err = GetWithDotNotation(i, "omega.zeta.epsilon")
	require.NoError(t, err)
	assert.Equal(t, 2.4, v.(float64))
}
