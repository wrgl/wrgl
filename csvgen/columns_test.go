// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneFifth(t *testing.T) {
	assert.Equal(t, 1, oneFifth(5))
	assert.Equal(t, 4, oneFifth(23))
	assert.Equal(t, 1, oneFifth(3))
}

func TestAddColumns(t *testing.T) {
	modifiedCols := map[string]struct{}{}
	rows := [][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	}
	rows = addColumns(modifiedCols, 2, rows)
	assert.Len(t, rows, 4)
	for _, sl := range rows {
		assert.Len(t, sl, 5)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	for name := range modifiedCols {
		found := false
		for _, s := range rows[0] {
			if s == name {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func TestRemColumns(t *testing.T) {
	modifiedCols := map[string]struct{}{"a": {}}
	rows := [][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}
	rows = remColumns(modifiedCols, 2, rows)
	assert.Len(t, rows, 4)
	for _, sl := range rows {
		assert.Len(t, sl, 2)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, modifiedCols, 1)
	foundA := false
	for _, s := range rows[0] {
		if s == "a" {
			foundA = true
			continue
		}
		_, ok := modifiedCols[s]
		assert.False(t, ok)
	}
	assert.True(t, foundA)
}

func TestRenameColumns(t *testing.T) {
	modifiedCols := map[string]struct{}{"a": {}}
	rows := [][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}
	rows = renameColumns(modifiedCols, 2, rows)
	assert.Len(t, rows, 4)
	for _, sl := range rows {
		assert.Len(t, sl, 4)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, modifiedCols, 5)
	_, ok := modifiedCols["a"]
	assert.True(t, ok)
	found := 0
	for _, s := range rows[0] {
		_, ok := modifiedCols[s]
		if ok {
			found++
			if s != "a" {
				assert.Len(t, s, 8)
				assert.True(t, strings.HasPrefix(s, "col_"))
			}
		}
	}
	assert.Equal(t, 3, found)
}

func TestMoveColumns(t *testing.T) {
	modifiedCols := map[string]struct{}{"a": {}}
	rows := [][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}
	for i, s := range rows[0] {
		assert.Equal(t, i, int(s[0]-97))
	}
	rows = moveColumns(modifiedCols, 1, rows)
	assert.Len(t, rows, 4)
	for _, sl := range rows {
		assert.Len(t, sl, 4)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, modifiedCols, 2)
	for name := range modifiedCols {
		if name == "a" {
			continue
		}
		for i, s := range rows[0] {
			if s == name {
				assert.NotEqual(t, i, int(s[0]-97))
			}
		}
	}
}

func TestGenColumns(t *testing.T) {
	assert.Equal(t, []string{}, genColumns(0))
	assert.Equal(t, []string{"col_a", "col_b", "col_c"}, genColumns(3))
	assert.Equal(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h",
		"col_i", "col_j", "col_k", "col_l", "col_m", "col_n", "col_o", "col_p",
		"col_q", "col_r", "col_s", "col_t", "col_u", "col_v", "col_w", "col_x",
		"col_y", "col_ba", "col_bb", "col_bc", "col_bd", "col_be",
	}, genColumns(30))
}
