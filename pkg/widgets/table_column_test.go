// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableColumn(t *testing.T) {
	tc := newTableColumn()
	assert.Equal(t, 0, tc.Expansion)
	tc.UpdateWidths([]int{20})
	tc.UpdateExpansions([]int{1})
	assert.Equal(t, []int{20}, tc.Widths())
	assert.Equal(t, 1, tc.Expansion)
	tc.UpdateWidths([]int{7, 7})
	tc.UpdateExpansions([]int{1, 1})
	assert.Equal(t, []int{7, 7}, tc.Widths())
	tc.DistributeWidth()
	assert.Equal(t, []int{9, 10}, tc.Widths())
	assert.Equal(t, []int{-1}, tc.CellWidths(0))
	assert.Equal(t, []int{20}, tc.CellWidths(1))
	assert.Equal(t, []int{9, 10}, tc.CellWidths(2))
	tc.DistributeExpansionWidth(4)
	assert.Equal(t, []int{11, 12}, tc.Widths())
	tc.UpdateWidths([]int{14, 16})
	tc.DistributeWidth()
	assert.Equal(t, 31, tc.Width)
}
