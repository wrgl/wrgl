package diff

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

type column struct {
	name           string
	added, removed bool
	movedFrom      int
}

func detectMovedColumns(cols []*column, origCols []string) []*column {
	origMap := map[string]int{}
	for ind, v := range origCols {
		origMap[v] = ind
	}
	newMap := map[string]int{}
	for ind, v := range cols {
		newMap[v.name] = ind
	}
	oldIndices := []int{}
	newIndices := []int{}
	for i, v := range cols {
		if _, ok := origMap[v.name]; !ok {
			continue
		}
		newIndices = append(newIndices, i)
		oldIndices = append(oldIndices, origMap[v.name])
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
			cols[newIndex].movedFrom = newMap[after] + 1
			if cols[newIndex].movedFrom > len(cols) {
				cols[newIndex].movedFrom = len(cols)
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
			cols[newIndex].movedFrom = newMap[before] - 1
			if cols[newIndex].movedFrom < 0 {
				cols[newIndex].movedFrom = 0
			}
		}
	}
	return cols
}

// export const compareColumns = (oldCols, newCols) => {
//   const result = [];
//   const oldMap = _.fromPairs(oldCols.map((v, ind) => [v, ind]));
//   const newMap = _.fromPairs(newCols.map((v, ind) => [v, ind]));
//   for (let name of newCols) {
//     if (oldMap[name] !== undefined) {
//       result.push({ name });
//     } else {
//       result.push({ name, added: true });
//     }
//   }

//   let anchor = 0;
//   let removedCols = [];
//   for (let i = 0; i < oldCols.length; i++) {
//     const name = oldCols[i];
//     if (newMap[name] !== undefined) {
//       anchor = newMap[name];
//       continue;
//     }
//     removedCols.push({ name, anchor });
//   }
//   removedCols = _.sortBy(removedCols, v => -v.anchor);
//   for (let { name, anchor } of removedCols) {
//     result.splice(anchor + 1, 0, { name, removed: true });
//   }

//   const commonCols = _.without(oldCols, ...removedCols.map(v => v.name));
//   return detectMovedColumns(result, commonCols);
// };
