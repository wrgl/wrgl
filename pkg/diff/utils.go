// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

func hoistPKTobeginning(cols []*RowChangeColumn, pk []string) []*RowChangeColumn {
	pkm := map[string]struct{}{}
	for _, s := range pk {
		pkm[s] = struct{}{}
	}
	pkIndices := []int{}
	ordinaryCols := []int{}
	for i, c := range cols {
		if _, ok := pkm[c.Name]; !ok {
			ordinaryCols = append(ordinaryCols, i)
		} else {
			pkIndices = append(pkIndices, i)
		}
	}
	result := []*RowChangeColumn{}
	for _, i := range append(pkIndices, ordinaryCols...) {
		result = append(result, cols[i])
	}
	return result
}

func stringSliceToMap(sl []string) map[string]int {
	m := map[string]int{}
	for i, s := range sl {
		m[s] = i
	}
	return m
}

func combineRows(cols []*RowChangeColumn, rowIndices, oldRowIndices map[string]int, row, oldRow []string) (mergedRows [][]string) {
	for _, col := range cols {
		if col.Added {
			mergedRows = append(mergedRows, []string{row[rowIndices[col.Name]]})
		} else if col.Removed {
			mergedRows = append(mergedRows, []string{oldRow[oldRowIndices[col.Name]]})
		} else if row[rowIndices[col.Name]] == oldRow[oldRowIndices[col.Name]] {
			mergedRows = append(mergedRows, []string{oldRow[oldRowIndices[col.Name]]})
		} else {
			mergedRows = append(mergedRows, []string{
				row[rowIndices[col.Name]], oldRow[oldRowIndices[col.Name]],
			})
		}
	}
	return
}
