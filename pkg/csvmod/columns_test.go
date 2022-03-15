// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package csvmod

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddColumns(t *testing.T) {
	m := NewModifier([][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	})
	m.AddColumns(2.0 / 3.0)
	assert.Len(t, m.Rows, 4)
	for _, sl := range m.Rows {
		assert.Len(t, sl, 5)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	for name := range m.modifiedCols {
		found := false
		for _, s := range m.Rows[0] {
			if s == name {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func TestRemColumns(t *testing.T) {
	m := NewModifier([][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}).PreserveColumns([]string{"a"})
	m.RemColumns(2.0 / 4.0)
	assert.Len(t, m.Rows, 4)
	for _, sl := range m.Rows {
		assert.Len(t, sl, 2)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, m.modifiedCols, 1)
	foundA := false
	for _, s := range m.Rows[0] {
		if s == "a" {
			foundA = true
			continue
		}
		_, ok := m.modifiedCols[s]
		assert.False(t, ok)
	}
	assert.True(t, foundA)
}

func TestRenameColumns(t *testing.T) {
	m := NewModifier([][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}).PreserveColumns([]string{"a"})
	m.RenameColumns(2.0 / 4.0)
	assert.Len(t, m.Rows, 4)
	for _, sl := range m.Rows {
		assert.Len(t, sl, 4)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, m.modifiedCols, 5)
	_, ok := m.modifiedCols["a"]
	assert.True(t, ok)
	found := 0
	for _, s := range m.Rows[0] {
		_, ok := m.modifiedCols[s]
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
	m := NewModifier([][]string{
		{"a", "b", "c", "d"},
		{"1", "q", "w", "e"},
		{"2", "a", "s", "d"},
		{"3", "z", "x", "c"},
	}).PreserveColumns([]string{"a"})
	m.MoveColumns(1.0 / 3.0)
	assert.Len(t, m.Rows, 4)
	for _, sl := range m.Rows {
		assert.Len(t, sl, 4)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, m.modifiedCols, 2)
	for name := range m.modifiedCols {
		if name == "a" {
			continue
		}
		for i, s := range m.Rows[0] {
			if s == name {
				assert.NotEqual(t, i, int(s[0]-97))
			}
		}
	}
}
