// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableColumn(t *testing.T) {
	tc := newTableColumn()
	tc.UpdateWidths(0, []int{10})
	tc.UpdateWidths(5, []int{20})
	tc.UpdateExpansions(0, []int{1})
	tc.UpdateWidths(11, []int{10, 30})
	tc.UpdateExpansions(11, []int{1, 2})
	tc.UpdateWidths(15, []int{20, 10, 10})
	tc.UpdateExpansions(15, []int{0, 0, 0})
	assert.Equal(t, []int{20}, tc.Widths(0))
	assert.Equal(t, []int{20}, tc.Widths(10))
	assert.Equal(t, []int{10, 30}, tc.Widths(11))
	assert.Equal(t, []int{10, 30}, tc.Widths(12))
	assert.Equal(t, []int{20, 10, 10}, tc.Widths(15))
	assert.Equal(t, []int{20, 10, 10}, tc.Widths(20))

	tc.DistributeWidth()
	assert.Equal(t, 42, tc.Width)
	assert.Equal(t, []int{42}, tc.Widths(0))
	assert.Equal(t, []int{10, 31}, tc.Widths(11))
	assert.Equal(t, []int{20, 10, 10}, tc.Widths(15))

	tc.DistributeExpansionWidth(30)
	assert.Equal(t, 72, tc.Width)
	assert.Equal(t, []int{72}, tc.Widths(0))
	assert.Equal(t, []int{20, 51}, tc.Widths(11))
	assert.Equal(t, []int{30, 20, 20}, tc.Widths(15))

	// tc.UpdateWidths([]int{7, 7})
	// tc.UpdateExpansions([]int{1, 1})
	// assert.Equal(t, []int{7, 7}, tc.Widths())
	// tc.DistributeWidth()
	// assert.Equal(t, []int{9, 10}, tc.Widths())
	// assert.Equal(t, []int{-1}, tc.CellWidths(0))
	// assert.Equal(t, []int{20}, tc.CellWidths(1))
	// assert.Equal(t, []int{9, 10}, tc.CellWidths(2))
	// tc.DistributeExpansionWidth(4)
	// assert.Equal(t, []int{11, 12}, tc.Widths())
	// tc.UpdateWidths([]int{14, 16})
	// tc.DistributeWidth()
	// assert.Equal(t, 31, tc.Width)
}
