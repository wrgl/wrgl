// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSliceToMap(t *testing.T) {
	assert.Equal(t, map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}, stringSliceToMap([]string{"a", "b", "c"}))
}

func TestCombineRows(t *testing.T) {
	cols, oldCols := []string{"a", "b", "c", "d", "e"}, []string{"e", "b", "c", "d", "f"}
	colChanges := CompareColumns(oldCols, cols)
	rowIndices := stringSliceToMap(cols)
	oldRowIndices := stringSliceToMap(oldCols)
	assert.Equal(t, []string{
		"a", "b", "c", "d", "f", "e",
	}, colChanges.Names())
	assert.True(t, colChanges.Added(0, 0))
	assert.True(t, colChanges.Removed(0, 4))
	b, a := colChanges.Moved(0, 5)
	assert.Equal(t, [2]int{1, -1}, [2]int{b, a})
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
			combineRows(colChanges, 0, rowIndices, oldRowIndices, c.row, c.oldRow),
			"case %d", i,
		)
	}
}
