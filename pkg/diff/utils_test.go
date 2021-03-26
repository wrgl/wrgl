package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHoistPKToBeginning(t *testing.T) {
	assert.Equal(t, []*RowChangeColumn{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}, hoistPKTobeginning(
		[]*RowChangeColumn{
			{Name: "b"},
			{Name: "a"},
			{Name: "c"},
		},
		[]string{"a"},
	))
}

func TestStringSliceToMap(t *testing.T) {
	assert.Equal(t, map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}, stringSliceToMap([]string{"a", "b", "c"}))
}

func TestCombineRows(t *testing.T) {
	cols, oldCols := []string{"a", "b", "c", "d", "e"}, []string{"e", "b", "c", "d", "f"}
	rowChangeCols := compareColumns(oldCols, cols)
	rowIndices := stringSliceToMap(cols)
	oldRowIndices := stringSliceToMap(oldCols)
	assert.Equal(t, []*RowChangeColumn{
		{Name: "a", Added: true, MovedFrom: -1},
		{Name: "b", MovedFrom: -1},
		{Name: "c", MovedFrom: -1},
		{Name: "d", MovedFrom: -1},
		{Name: "f", Removed: true, MovedFrom: -1},
		{Name: "e", MovedFrom: 0},
	}, rowChangeCols)
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
			combineRows(rowChangeCols, rowIndices, oldRowIndices, c.row, c.oldRow),
			"case %d", i,
		)
	}
}
