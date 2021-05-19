// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

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
	}
	i := &myType{
		Alpha: "abc",
		Beta: struct {
			Gamma int
		}{30},
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
}
