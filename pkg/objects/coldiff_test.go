// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func intSliceToMap(sl []uint32) map[uint32]struct{} {
	m := map[uint32]struct{}{}
	for _, i := range sl {
		m[i] = struct{}{}
	}
	return m
}

func TestLongestIncreasingList(t *testing.T) {
	for i, c := range []struct {
		sl, res []int
	}{
		{[]int{}, []int{}},
		{[]int{0}, []int{0}},
		{[]int{0, 1}, []int{0, 1}},
		{[]int{1, 0}, []int{0}},
		{[]int{2, 0, 1}, []int{1, 2}},
		{[]int{1, 2, 0}, []int{0, 1}},
		{[]int{1, 0, 2}, []int{0, 2}},
		{[]int{0, 1, 2}, []int{0, 1, 2}},
		{[]int{2, 1, 0}, []int{1}},
		{[]int{0, 4, 5, 1, 2, 3}, []int{0, 3, 4, 5}},
		{[]int{0, 4, 5, 1, 2}, []int{0, 1, 2}},
		{[]int{4, 5, 0, 2, 1, 3}, []int{2, 3, 5}},
	} {
		assert.Equal(t, c.res, longestIncreasingList(c.sl), "case %d", i)
	}
}

func TestMoveOps(t *testing.T) {
	for i, c := range []struct {
		sl  []int
		ops []*moveOp
	}{
		{[]int{}, []*moveOp{}},
		{[]int{0}, []*moveOp{}},
		{[]int{0, 1}, []*moveOp{}},
		{[]int{0, 1, 2}, []*moveOp{}},
		{[]int{1, 0}, []*moveOp{{old: 0, new: 1}}},
		{[]int{1, 0, 2}, []*moveOp{{old: 0, new: 1}}},
		{[]int{1, 2, 0}, []*moveOp{{old: 0, new: 2}}},
		{[]int{2, 1, 0}, []*moveOp{
			{old: 2, new: 0},
			{old: 0, new: 2},
		}},
		{[]int{0, 1, 2, 5, 3, 4}, []*moveOp{{old: 5, new: 3}}},
		{[]int{2, 1, 4, 5, 3, 0}, []*moveOp{
			{old: 1, new: 1},
			{old: 3, new: 4},
			{old: 0, new: 5},
		}},
	} {
		assert.Equal(t, c.ops, moveOps(c.sl), "case %d", i)
	}
}

func TestColumns(t *testing.T) {
	for i, c := range []struct {
		base     []string
		cols     []string
		names    []string
		moved    map[uint32][]int
		added    []uint32
		removed  []uint32
		baseIdx  map[uint32]uint32
		otherIdx map[uint32]uint32
	}{
		{
			base:     []string{"a"},
			cols:     []string{"a"},
			names:    []string{"a"},
			baseIdx:  map[uint32]uint32{0: 0},
			otherIdx: map[uint32]uint32{0: 0},
		},
		{
			base:     []string{"a"},
			cols:     []string{"b"},
			names:    []string{"a", "b"},
			added:    []uint32{1},
			removed:  []uint32{0},
			baseIdx:  map[uint32]uint32{0: 0},
			otherIdx: map[uint32]uint32{1: 0},
		},
		{
			base:     []string{"a", "b"},
			cols:     []string{"a", "b"},
			names:    []string{"a", "b"},
			baseIdx:  map[uint32]uint32{0: 0, 1: 1},
			otherIdx: map[uint32]uint32{0: 0, 1: 1},
		},
		{
			base:     []string{"a", "b"},
			cols:     []string{"b", "a"},
			names:    []string{"b", "a"},
			moved:    map[uint32][]int{1: {0, -1}},
			baseIdx:  map[uint32]uint32{0: 1, 1: 0},
			otherIdx: map[uint32]uint32{0: 0, 1: 1},
		},
		{
			base:     []string{"a", "b", "c"},
			cols:     []string{"c", "a"},
			names:    []string{"c", "a", "b"},
			moved:    map[uint32][]int{1: {0, -1}},
			removed:  []uint32{2},
			baseIdx:  map[uint32]uint32{0: 2, 1: 0, 2: 1},
			otherIdx: map[uint32]uint32{0: 0, 1: 1},
		},
		{
			base:     []string{"c", "b", "a"},
			cols:     []string{"a", "b", "c"},
			names:    []string{"a", "b", "c"},
			moved:    map[uint32][]int{0: {-1, 1}, 2: {1, -1}},
			baseIdx:  map[uint32]uint32{0: 2, 1: 1, 2: 0},
			otherIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2},
		},
		{
			base:     []string{"a", "b"},
			cols:     []string{"b", "c", "a"},
			names:    []string{"b", "c", "a"},
			moved:    map[uint32][]int{2: {0, -1}},
			added:    []uint32{1},
			baseIdx:  map[uint32]uint32{0: 1, 2: 0},
			otherIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2},
		},
		{
			base:     []string{"a", "d", "e", "b", "c"},
			cols:     []string{"a", "b", "c", "d", "e"},
			names:    []string{"a", "b", "c", "d", "e"},
			moved:    map[uint32][]int{3: {-1, 0}, 4: {-1, 0}},
			baseIdx:  map[uint32]uint32{0: 0, 1: 3, 2: 4, 3: 1, 4: 2},
			otherIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2, 3: 3, 4: 4},
		},
		{
			base:     []string{"e", "b", "c", "d", "f"},
			cols:     []string{"a", "b", "c", "d", "e"},
			names:    []string{"a", "b", "c", "d", "f", "e"},
			moved:    map[uint32][]int{5: {1, -1}},
			added:    []uint32{0},
			removed:  []uint32{4},
			baseIdx:  map[uint32]uint32{1: 1, 2: 2, 3: 3, 4: 4, 5: 0},
			otherIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2, 3: 3, 5: 4},
		},
	} {
		obj := CompareColumns([2][]string{c.base, nil}, [2][]string{c.cols, nil})
		n := obj.Len()
		assert.Equal(t, len(c.names), n, "case %d", i)
		assert.Equal(t, c.names, obj.Names, "case %d", i)
		assert.Equal(t, c.baseIdx, obj.BaseIdx, "case %d", i)
		assert.Equal(t, c.otherIdx, obj.OtherIdx[0], "case %d", i)

		assert.Equal(t, intSliceToMap(c.added), obj.Added[0], "case %d", i)
		assert.Equal(t, intSliceToMap(c.removed), obj.Removed[0], "case %d", i)
		if c.moved == nil {
			assert.Len(t, obj.Moved[0], 0, "case %d", i)
		} else {
			assert.Equal(t, c.moved, obj.Moved[0], "case %d", i)
		}
		assert.Nil(t, obj.BasePK, "case %d", i)
		assert.Equal(t, make([][]uint32, obj.Layers()), obj.OtherPK, "case %d", i)
	}
}

func TestHoistPKToStart(t *testing.T) {
	c := CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [2][]string{{"b", "a", "d"}, {"d", "a"}})
	assert.Equal(t, []string{"d", "a", "b", "c"}, c.Names)
	assert.Equal(t, []uint32{0, 1}, c.PKIndices())
	assert.Equal(t, []uint32{1}, c.BasePK)
	assert.Equal(t, [][]uint32{{0, 1}}, c.OtherPK)
	assert.Equal(t, []string{"", "1", "2", "3"}, c.RearrangeBaseRow([]string{"1", "2", "3"}))
	assert.Equal(t, []string{"3", "2", "1", ""}, c.RearrangeRow(0, []string{"1", "2", "3"}))

	c = CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [2][]string{{"a", "b", "c"}, {"a"}})
	assert.Equal(t, []string{"a", "b", "c"}, c.Names)
	assert.Equal(t, []uint32{0}, c.PKIndices())
	assert.Equal(t, []uint32{0}, c.BasePK)
	assert.Equal(t, [][]uint32{{0}}, c.OtherPK)
	assert.Equal(t, []string{"1", "2", "3"}, c.RearrangeBaseRow([]string{"1", "2", "3"}))
	assert.Equal(t, []string{"1", "2", "3"}, c.RearrangeRow(0, []string{"1", "2", "3"}))
}

func TestColumnsSwap(t *testing.T) {
	c := &ColDiff{
		Names:   []string{"b", "c", "a", "d"},
		Removed: []map[uint32]struct{}{{1: struct{}{}}},
		Added:   []map[uint32]struct{}{{3: struct{}{}}},
		Moved:   []map[uint32][]int{{2: []int{0, -1}}},
	}

	c.Swap(0, 3)
	assert.Equal(t, []string{"d", "c", "a", "b"}, c.Names)
	_, ok := c.Added[0][3]
	assert.False(t, ok)
	_, ok = c.Added[0][0]
	assert.True(t, ok)

	c.Swap(1, 2)
	assert.Equal(t, []string{"d", "a", "c", "b"}, c.Names)
	_, ok = c.Removed[0][1]
	assert.False(t, ok)
	_, ok = c.Removed[0][2]
	assert.True(t, ok)
	v, ok := c.Moved[0][1]
	assert.True(t, ok)
	assert.Equal(t, []int{0, -1}, v)
	_, ok = c.Moved[0][2]
	assert.False(t, ok)
}

func TestCombineRows(t *testing.T) {
	cols, oldCols := []string{"a", "b", "c", "d", "e"}, []string{"e", "b", "c", "d", "f"}
	colDiff := CompareColumns([2][]string{oldCols, nil}, [2][]string{cols, nil})
	assert.Equal(t, []string{
		"a", "b", "c", "d", "f", "e",
	}, colDiff.Names)
	_, ok := colDiff.Added[0][0]
	assert.True(t, ok)
	_, ok = colDiff.Removed[0][4]
	assert.True(t, ok)
	v, ok := colDiff.Moved[0][5]
	assert.True(t, ok)
	assert.Equal(t, []int{1, -1}, v)
	for i, c := range []struct {
		row, oldRow []string
		mergedRows  [][]string
	}{
		{
			[]string{"1", "2", "3", "4", "5"},
			[]string{"6", "2", "7", "4", "5"},
			[][]string{
				{"1"}, {"2"}, {"3", "7"}, {"4"}, {"5"}, {"5", "6"},
			},
		},
	} {
		assert.Equal(t, c.mergedRows,
			colDiff.CombineRows(0, c.row, c.oldRow),
			"case %d", i,
		)
	}
}
