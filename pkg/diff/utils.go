// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

func stringSliceToMap(sl []string) map[string]int {
	m := map[string]int{}
	for i, s := range sl {
		m[s] = i
	}
	return m
}

func combineRows(cols *Columns, layer int, rowIndices, oldRowIndices map[string]int, row, oldRow []string) (mergedRows [][]string) {
	n := cols.Len()
	for i := 0; i < n; i++ {
		name := cols.Name(i)
		if cols.Added(layer, i) {
			mergedRows = append(mergedRows, []string{row[rowIndices[name]]})
		} else if cols.Removed(layer, i) {
			mergedRows = append(mergedRows, []string{oldRow[oldRowIndices[name]]})
		} else if row[rowIndices[name]] == oldRow[oldRowIndices[name]] {
			mergedRows = append(mergedRows, []string{oldRow[oldRowIndices[name]]})
		} else {
			mergedRows = append(mergedRows, []string{
				row[rowIndices[name]], oldRow[oldRowIndices[name]],
			})
		}
	}
	return
}
