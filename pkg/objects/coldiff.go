// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"container/list"
	"sort"
)

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

func stringSliceToMap(sl []string) map[string]int {
	m := map[string]int{}
	for i, s := range sl {
		m[s] = i
	}
	return m
}

func uintMapToSlice(m map[uint32]struct{}) []uint32 {
	sl := make([]uint32, 0, len(m))
	for u := range m {
		sl = append(sl, u)
	}
	return sl
}

// ColDiff keep track of how column composition and order change between a base version and
// one or more versions
type ColDiff struct {
	Names    []string
	BasePK   []uint32
	OtherPK  [][]uint32
	pk       map[string]int
	Added    []map[uint32]struct{}
	Removed  []map[uint32]struct{}
	Moved    []map[uint32][]int
	BaseIdx  map[uint32]uint32
	OtherIdx []map[uint32]uint32
}

func CompareColumns(base [2][]string, others ...[2][]string) *ColDiff {
	c := &ColDiff{}
	for _, sl := range others {
		c.addLayer(base[0], sl[0])
	}
	c.hoistPKToStart(others[0][1])
	c.computeIndexMap(base, others...)
	return c
}

func (c *ColDiff) Layers() int {
	return len(c.Added)
}

func (c *ColDiff) PKIndices() []uint32 {
	n := len(c.OtherPK[0])
	sl := make([]uint32, n)
	for i := 0; i < n; i++ {
		sl[i] = uint32(i)
	}
	return sl
}

func (c *ColDiff) PK() []string {
	vals := make([]string, len(c.OtherPK[0]))
	for i, j := range c.OtherPK[0] {
		vals[i] = c.Names[j]
	}
	return vals
}

func (c *ColDiff) computeIndexMap(base [2][]string, others ...[2][]string) {
	c.BaseIdx = map[uint32]uint32{}
	namesM := stringSliceToMap(c.Names)
	for i, s := range base[0] {
		c.BaseIdx[uint32(namesM[s])] = uint32(i)
	}
	for _, s := range base[1] {
		c.BasePK = append(c.BasePK, uint32(namesM[s]))
	}
	n := len(others)
	c.OtherIdx = make([]map[uint32]uint32, n)
	c.OtherPK = make([][]uint32, n)
	for j, layer := range others {
		c.OtherIdx[j] = map[uint32]uint32{}
		for i, s := range layer[0] {
			c.OtherIdx[j][uint32(namesM[s])] = uint32(i)
		}
		for _, s := range layer[1] {
			c.OtherPK[j] = append(c.OtherPK[j], uint32(namesM[s]))
		}
	}
}

func (c *ColDiff) CombineRows(layer int, row, oldRow []string) (mergedRows [][]string) {
	n := uint32(c.Len())
	var i uint32
	for i = 0; i < n; i++ {
		if _, ok := c.Added[layer][i]; ok {
			mergedRows = append(mergedRows, []string{row[c.OtherIdx[layer][i]]})
		} else if _, ok := c.Removed[layer][i]; ok {
			mergedRows = append(mergedRows, []string{oldRow[c.BaseIdx[i]]})
		} else if row[c.OtherIdx[layer][i]] == oldRow[c.BaseIdx[i]] {
			mergedRows = append(mergedRows, []string{oldRow[c.BaseIdx[i]]})
		} else {
			mergedRows = append(mergedRows, []string{
				row[c.OtherIdx[layer][i]], oldRow[c.BaseIdx[i]],
			})
		}
	}
	return
}

func (c *ColDiff) RearrangeRow(layer int, row []string) []string {
	n := c.Len()
	res := make([]string, n)
	for i := 0; i < n; i++ {
		if j, ok := c.OtherIdx[layer][uint32(i)]; ok {
			res[i] = row[j]
		}
	}
	return res
}

func (c *ColDiff) RearrangeBaseRow(row []string) []string {
	n := c.Len()
	res := make([]string, n)
	for i := 0; i < n; i++ {
		if j, ok := c.BaseIdx[uint32(i)]; ok {
			res[i] = row[j]
		}
	}
	return res
}

func (c *ColDiff) Swap(i, j int) {
	u := uint32(i)
	v := uint32(j)
	c.Names[i], c.Names[j] = c.Names[j], c.Names[i]
	for layer := 0; layer < c.Layers(); layer++ {
		for _, sl := range [][]map[uint32]struct{}{c.Added, c.Removed} {
			_, oki := sl[layer][u]
			_, okj := sl[layer][v]
			if oki && !okj {
				sl[layer][v] = struct{}{}
				delete(sl[layer], u)
			} else if okj && !oki {
				sl[layer][u] = struct{}{}
				delete(sl[layer], v)
			}
		}
		m := c.Moved[layer]
		vi, oki := m[u]
		vj, okj := m[v]
		if oki && okj {
			m[u], m[v] = vj, vi
		} else if oki {
			m[v] = vi
			delete(m, u)
		} else if okj {
			m[u] = vj
			delete(m, v)
		}
	}
}

func (c *ColDiff) Len() int {
	return len(c.Names)
}

func (c *ColDiff) Less(i, j int) bool {
	if c.pk == nil {
		return false
	}
	vi, oki := c.pk[c.Names[i]]
	vj, okj := c.pk[c.Names[j]]
	if oki && okj {
		return vi < vj
	} else if oki {
		return true
	}
	return false
}

func (c *ColDiff) insertToNames(cols []string) {
	namesM := stringSliceToMap(c.Names)
	n := len(c.Names)
	var anchor = -1
	type offList struct {
		Off int
		L   *list.List
	}
	listM := map[int]*offList{}
	lists := []*offList{}
	total := 0
	for _, s := range cols {
		if i, ok := namesM[s]; ok {
			anchor = i
			continue
		}
		l, ok := listM[anchor]
		if !ok {
			listM[anchor] = &offList{anchor, list.New()}
			l = listM[anchor]
			lists = append(lists, l)
		}
		l.L.PushBack(s)
		total++
	}
	if total == 0 {
		return
	}
	names := make([]string, n+total)
	prevAnchor := 0
	off := 0
	sort.Slice(lists, func(i, j int) bool {
		return lists[i].Off < lists[j].Off
	})
	for _, obj := range lists {
		copy(names[off:], c.Names[prevAnchor:obj.Off+1])
		off += obj.Off + 1 - prevAnchor
		for e := obj.L.Front(); e != nil; e = e.Next() {
			names[off] = e.Value.(string)
			off++
		}
		prevAnchor = obj.Off + 1
	}
	if prevAnchor < n {
		copy(names[off:], c.Names[prevAnchor:])
	}
	c.Names = names
}

func (c *ColDiff) addLayer(base, cols []string) {
	layer := c.Layers()
	c.Added = append(c.Added, map[uint32]struct{}{})
	c.Removed = append(c.Removed, map[uint32]struct{}{})
	c.Moved = append(c.Moved, map[uint32][]int{})

	c.insertToNames(cols)
	c.insertToNames(base)

	// populate added map
	baseM := stringSliceToMap(base)
	namesM := stringSliceToMap(c.Names)
	for _, s := range cols {
		if _, ok := baseM[s]; !ok {
			c.Added[layer][uint32(namesM[s])] = struct{}{}
		}
	}

	// populate removed map
	colsM := stringSliceToMap(cols)
	for _, s := range base {
		if _, ok := colsM[s]; !ok {
			c.Removed[layer][uint32(namesM[s])] = struct{}{}
		}
	}

	c.populateMovedMap(base, layer, colsM)
}

func (c *ColDiff) populateMovedMap(base []string, layer int, colsM map[string]int) {
	common := []string{}
	for _, s := range base {
		if _, ok := colsM[s]; ok {
			common = append(common, s)
		}
	}
	commonM := stringSliceToMap(common)
	oldIndices := []int{}
	newIndices := []int{}
	for i, s := range c.Names {
		if j, ok := commonM[s]; ok {
			newIndices = append(newIndices, i)
			oldIndices = append(oldIndices, j)
		}
	}
	ops := moveOps(oldIndices)
	nonAnchor := map[int]struct{}{}
	for _, v := range ops {
		nonAnchor[(v.old)] = struct{}{}
	}
	namesM := stringSliceToMap(c.Names)
	for _, op := range ops {
		newIndex := uint32(newIndices[op.new])
		var after string
		for i := op.old - 1; i >= 0; i-- {
			if _, ok := nonAnchor[i]; ok {
				continue
			}
			after = common[i]
			if _, ok := namesM[after]; ok {
				break
			}
		}
		if after != "" {
			c.Moved[layer][newIndex] = []int{-1, namesM[after]}
			continue
		}
		// search for anchor column after this column
		var before string
		for i := op.old + 1; i < len(common); i++ {
			if _, ok := nonAnchor[i]; ok {
				continue
			}
			before = common[i]
			if _, ok := namesM[before]; ok {
				break
			}
		}
		if before != "" {
			c.Moved[layer][newIndex] = []int{namesM[before], -1}
		}
	}
}

func (c *ColDiff) hoistPKToStart(pk []string) {
	c.pk = stringSliceToMap(pk)
	sort.Stable(c)
	c.pk = nil
}
