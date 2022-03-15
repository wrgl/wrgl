// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package widgets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCellsPool(t *testing.T) {
	vt := NewVirtualTable()
	cp := NewCellsPool(vt)

	cells, found := cp.Get(0, 0, 1)
	assert.False(t, found)
	assert.Len(t, cells, 1)
	cells[0].SetText("abc")

	cells, found = cp.Get(0, 0, 1)
	assert.True(t, found)
	assert.Equal(t, "abc", cells[0].Text)

	cells, found = cp.Get(0, 0, 2)
	assert.False(t, found)
	assert.Len(t, cells, 2)
	assert.Equal(t, "", cells[0].Text)

	cells, found = cp.Get(3, 3, 3)
	assert.False(t, found)
	assert.Len(t, cells, 3)

	vt.onVisibleCellsChange([]int{3, 4, 5}, []int{0, 1, 2, 3})
	_, found = cp.Get(0, 0, 2)
	assert.False(t, found)
	_, found = cp.Get(3, 3, 3)
	assert.True(t, found)
	_, found = cp.Get(4, 1, 1)
	assert.False(t, found)
}
