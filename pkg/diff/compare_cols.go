package diff

import "sort"

// longestIncreasingList returns indices of longest increasing values
func longestIncreasingList(sl []int) []int {
	type node struct {
		ind  int
		prev *node
		len  int
	}
	nodes := map[int]*node{}
	var root *node
	for i, v := range sl {
		var prev *node
		for j := v - 1; j >= 0; j-- {
			if nodes[j] != nil && (prev == nil || prev.len < nodes[j].len) {
				prev = nodes[j]
			}
			if prev != nil && j < prev.len {
				break
			}
		}
		nodes[v] = &node{ind: i, prev: prev, len: 1}
		if prev != nil {
			nodes[v].len = prev.len + 1
		}
		if root == nil || root.len < nodes[v].len || (root.len == nodes[v].len && v == i) {
			root = nodes[v]
		}
	}
	results := []int{}
	for root != nil {
		results = append([]int{root.ind}, results...)
		root = root.prev
	}
	return results
}

type moveOp struct {
	old, new int
}

// moveOps returns move operations that changed order of array indices
func moveOps(sl []int) []*moveOp {
	anchorIndices := longestIncreasingList(sl)
	for _, i := range anchorIndices {
		sl[i] = -1
	}
	ops := []*moveOp{}
	for i, v := range sl {
		if v == -1 {
			continue
		}
		ops = append(ops, &moveOp{v, i})
	}
	return ops
}

type RowChangeColumn struct {
	Name      string `json:"name"`
	Added     bool   `json:"added,omitempty"`
	Removed   bool   `json:"removed,omitempty"`
	anchor    int    `json:"-"`
	MovedFrom int    `json:"movedFrom,omitempty"`
}

func detectMovedColumns(cols []*RowChangeColumn, origCols []string) []*RowChangeColumn {
	origMap := map[string]int{}
	for ind, v := range origCols {
		origMap[v] = ind
	}
	newMap := map[string]int{}
	for ind, v := range cols {
		newMap[v.Name] = ind
	}
	oldIndices := []int{}
	newIndices := []int{}
	for i, v := range cols {
		if _, ok := origMap[v.Name]; !ok {
			continue
		}
		newIndices = append(newIndices, i)
		oldIndices = append(oldIndices, origMap[v.Name])
	}
	ops := moveOps(oldIndices)
	nonAnchor := map[int]struct{}{}
	for _, v := range ops {
		nonAnchor[v.old] = struct{}{}
	}
	for _, op := range ops {
		newIndex := newIndices[op.new]
		var after string
		for i := op.old - 1; i >= 0; i-- {
			if _, ok := nonAnchor[i]; ok {
				continue
			}
			after = origCols[i]
			if _, ok := newMap[after]; ok {
				break
			}
		}
		if after != "" {
			cols[newIndex].MovedFrom = newMap[after] + 1
			if cols[newIndex].MovedFrom > len(cols) {
				cols[newIndex].MovedFrom = len(cols)
			}
			continue
		}
		// search for anchor column after this column
		var before string
		for i := op.old + 1; i < len(origCols); i++ {
			if _, ok := nonAnchor[i]; ok {
				continue
			}
			before = origCols[i]
			if _, ok := newMap[before]; ok {
				break
			}
		}
		if before != "" {
			cols[newIndex].MovedFrom = newMap[before] - 1
			if cols[newIndex].MovedFrom < 0 {
				cols[newIndex].MovedFrom = 0
			}
		}
	}
	return cols
}

func compareColumns(oldCols, newCols []string) []*RowChangeColumn {
	result := []*RowChangeColumn{}
	oldMap := map[string]int{}
	for ind, v := range oldCols {
		oldMap[v] = ind
	}
	newMap := map[string]int{}
	for ind, v := range newCols {
		newMap[v] = ind
	}
	for _, name := range newCols {
		if _, ok := oldMap[name]; ok {
			result = append(result, &RowChangeColumn{Name: name})
		} else {
			result = append(result, &RowChangeColumn{Name: name, Added: true})
		}
	}

	anchor := 0
	removedCols := []*RowChangeColumn{}
	for _, name := range oldCols {
		if _, ok := newMap[name]; ok {
			anchor = newMap[name]
			continue
		}
		removedCols = append(removedCols, &RowChangeColumn{Name: name, anchor: anchor})
	}
	sort.Slice(removedCols, func(i, j int) bool {
		return removedCols[j].anchor < removedCols[i].anchor
	})
	for _, col := range removedCols {
		result = append(result[:col.anchor+1], result[col.anchor:]...)
		result[col.anchor+1] = &RowChangeColumn{Name: col.Name, Removed: true}
	}

	removedMap := map[string]int{}
	for ind, v := range removedCols {
		removedMap[v.Name] = ind
	}
	commonCols := []string{}
	for _, col := range oldCols {
		if _, ok := removedMap[col]; !ok {
			commonCols = append(commonCols, col)
		}
	}
	return detectMovedColumns(result, commonCols)
}
