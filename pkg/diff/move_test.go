// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLongestIncreasingList(t *testing.T) {
	assert.Equal(t, longestIncreasingList([]int{}), []int{})
	assert.Equal(t, longestIncreasingList([]int{0}), []int{0})
	assert.Equal(t, longestIncreasingList([]int{0, 1}), []int{0, 1})
	assert.Equal(t, longestIncreasingList([]int{1, 0}), []int{0})
	assert.Equal(t, longestIncreasingList([]int{2, 0, 1}), []int{1, 2})
	assert.Equal(t, longestIncreasingList([]int{1, 2, 0}), []int{0, 1})
	assert.Equal(t, longestIncreasingList([]int{1, 0, 2}), []int{0, 2})
	assert.Equal(t, longestIncreasingList([]int{0, 1, 2}), []int{0, 1, 2})
	assert.Equal(t, longestIncreasingList([]int{2, 1, 0}), []int{1})
	assert.Equal(t, longestIncreasingList([]int{0, 4, 5, 1, 2, 3}), []int{0, 3, 4, 5})
	assert.Equal(t, longestIncreasingList([]int{0, 4, 5, 1, 2}), []int{0, 1, 2})
	assert.Equal(t, longestIncreasingList([]int{4, 5, 0, 2, 1, 3}), []int{2, 3, 5})
}

func TestMoveOps(t *testing.T) {
	assert.Equal(t, moveOps([]int{}), []*moveOp{})
	assert.Equal(t, moveOps([]int{0}), []*moveOp{})
	assert.Equal(t, moveOps([]int{0, 1}), []*moveOp{})
	assert.Equal(t, moveOps([]int{0, 1, 2}), []*moveOp{})
	assert.Equal(t, moveOps([]int{1, 0}), []*moveOp{{old: 0, new: 1}})
	assert.Equal(t, moveOps([]int{1, 0, 2}), []*moveOp{{old: 0, new: 1}})
	assert.Equal(t, moveOps([]int{1, 2, 0}), []*moveOp{{old: 0, new: 2}})
	assert.Equal(t, moveOps([]int{2, 1, 0}), []*moveOp{
		{old: 2, new: 0},
		{old: 0, new: 2},
	})
	assert.Equal(t, moveOps([]int{0, 1, 2, 5, 3, 4}), []*moveOp{{old: 5, new: 3}})
	assert.Equal(t, moveOps([]int{2, 1, 4, 5, 3, 0}), []*moveOp{
		{old: 1, new: 1},
		{old: 3, new: 4},
		{old: 0, new: 5},
	})
}
