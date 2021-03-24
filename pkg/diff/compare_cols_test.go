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

func TestDetectMovedColumns(t *testing.T) {
	assert.Equal(t, detectMovedColumns([]*RowChangeColumn{{Name: "a"}}, []string{"a"}), []*RowChangeColumn{
		{Name: "a"},
	})
	assert.Equal(t, detectMovedColumns([]*RowChangeColumn{{Name: "a"}}, []string{"b"}), []*RowChangeColumn{
		{Name: "a"},
	})
	assert.Equal(t,
		detectMovedColumns([]*RowChangeColumn{{Name: "a"}, {Name: "b"}}, []string{"b"}),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b"}})
	assert.Equal(t,
		detectMovedColumns([]*RowChangeColumn{{Name: "a"}, {Name: "b"}}, []string{"a"}),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b"}})
	assert.Equal(t,
		detectMovedColumns([]*RowChangeColumn{{Name: "a"}, {Name: "b"}}, []string{"a", "b"}),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b"}})
	assert.Equal(t,
		detectMovedColumns([]*RowChangeColumn{{Name: "a"}, {Name: "b"}}, []string{"b", "a"}),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b", MovedFrom: 0}})
	assert.Equal(t,
		detectMovedColumns(
			[]*RowChangeColumn{{Name: "a"}, {Name: "b"}, {Name: "c"}},
			[]string{"c", "a"},
		),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b"}, {Name: "c", MovedFrom: 0}})
	assert.Equal(t,
		detectMovedColumns([]*RowChangeColumn{{Name: "a"}, {Name: "b"}}, []string{"b", "c", "a"}),
		[]*RowChangeColumn{{Name: "a"}, {Name: "b", MovedFrom: 0}})
	assert.Equal(t,
		detectMovedColumns(
			[]*RowChangeColumn{
				{Name: "a"},
				{Name: "d"},
				{Name: "e"},
				{Name: "b"},
				{Name: "c"},
			},
			[]string{"a", "b", "c", "d", "e"},
		),
		[]*RowChangeColumn{
			{Name: "a"},
			{Name: "d"},
			{Name: "e"},
			{Name: "b", MovedFrom: 1},
			{Name: "c", MovedFrom: 1},
		})
}

func TestCompareColumns(t *testing.T) {
	assert.Equal(t, compareColumns([]string{"a"}, []string{"a"}), []*RowChangeColumn{{Name: "a"}})
	assert.Equal(t, compareColumns([]string{"a", "b"}, []string{"a"}), []*RowChangeColumn{
		{Name: "a"},
		{Name: "b", Removed: true},
	})
	assert.Equal(t, compareColumns([]string{"a"}, []string{"a", "b"}), []*RowChangeColumn{
		{Name: "a"},
		{Name: "b", Added: true},
	})
	assert.Equal(t, compareColumns([]string{"b", "a"}, []string{"a", "b"}), []*RowChangeColumn{
		{Name: "a"},
		{Name: "b", MovedFrom: 0},
	})
	assert.Equal(t, compareColumns([]string{"c", "b", "a"}, []string{"a", "b", "c"}), []*RowChangeColumn{
		{Name: "a", MovedFrom: 2},
		{Name: "b"},
		{Name: "c", MovedFrom: 0},
	})
	assert.Equal(t,
		compareColumns([]string{"e", "b", "c", "d", "f"}, []string{"a", "b", "c", "d", "e"}),
		[]*RowChangeColumn{
			{Name: "a", Added: true},
			{Name: "b"},
			{Name: "c"},
			{Name: "d"},
			{Name: "f", Removed: true},
			{Name: "e", MovedFrom: 0},
		})
}
