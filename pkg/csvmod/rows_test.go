// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package csvmod

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRows(t *testing.T) {
	m := NewModifier([][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	})
	m.AddRows(2.0 / 3.0)
	assert.Len(t, m.Rows, 6)
	for _, sl := range m.Rows {
		assert.Len(t, sl, 3)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
	}
	assert.Len(t, m.modifiedRows, 2)
	for off := range m.modifiedRows {
		assert.True(t, len(m.Rows[off+1][0]) > 1)
	}
}

func TestRemoveRows(t *testing.T) {
	m := NewModifier([][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
		{"4", "r", "t"},
	})
	m.modifiedRows[1] = struct{}{}
	m.RemoveRows(2.0 / 4.0)
	found := false
	for _, sl := range m.Rows {
		assert.Len(t, sl, 3)
		for _, s := range sl {
			assert.NotEmpty(t, s)
		}
		if sl[0] == "2" {
			found = true
		}
	}
	assert.True(t, found)
	assert.Len(t, m.Rows, 3)
	assert.Len(t, m.modifiedRows, 1)
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
	m := NewModifier(cloneStringMatrix(rows))
	m.modifiedRows[0] = struct{}{}
	m.ModifyRows(2.0 / 4.0)
	assert.Len(t, m.Rows, 5)
	assert.Len(t, m.modifiedRows, 3)
	for i, sl := range m.Rows {
		assert.Len(t, sl, 3)
		_, ok := m.modifiedRows[i-1]
		if i <= 1 || !ok {
			assert.Equal(t, rows[i], sl)
		} else {
			assert.NotEqual(t, rows[i], sl)
		}
	}
}
