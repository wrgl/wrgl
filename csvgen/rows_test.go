// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRows(t *testing.T) {
	rows := [][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	}
	modRows := map[int]struct{}{}
	rows = addRows(modRows, 2, rows)
	assert.Len(t, rows, 6)
	for _, sl := range rows {
		assert.Len(t, sl, 3)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, modRows, 2)
	for off := range modRows {
		assert.True(t, len(rows[off+1][0]) > 1)
	}
}

func TestRemoveRows(t *testing.T) {
	rows := [][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
		{"4", "r", "t"},
	}
	modRows := map[int]struct{}{1: {}}
	rows = removeRows(modRows, 2, rows)
	found := false
	for _, sl := range rows {
		assert.Len(t, sl, 3)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
		if sl[0] == "2" {
			found = true
		}
	}
	assert.True(t, found)
	assert.Len(t, rows, 3)
	assert.Len(t, modRows, 1)
}

func cloneStringMatrix(sl [][]string) [][]string {
	res := make([][]string, len(sl))
	for i, row := range sl {
		res[i] = make([]string, len(row))
		for j, s := range row {
			res[i][j] = s
		}
	}
	return res
}

func TestModifyRows(t *testing.T) {
	rows := [][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
		{"4", "r", "t"},
	}
	modRows := map[int]struct{}{0: {}}
	rows2 := modifyRows(modRows, 2, cloneStringMatrix(rows))
	assert.Len(t, rows2, 5)
	assert.Len(t, modRows, 3)
	for i, sl := range rows2 {
		assert.Len(t, sl, 3)
		_, ok := modRows[i-1]
		if i <= 1 || !ok {
			assert.Equal(t, rows[i], sl)
		} else {
			assert.NotEqual(t, rows[i], sl)
		}
	}
}
