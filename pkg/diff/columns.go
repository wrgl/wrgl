// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"container/list"
	"sort"
)

// Columns keep track of how column composition and order change between a base version and
// one or more versions
type Columns struct {
	base    []string
	names   []string
	pk      map[string]int
	added   []map[int]struct{}
	removed []map[int]struct{}
	moved   []map[int][2]int
}

func CompareColumns(base []string, others ...[]string) *Columns {
	c := &Columns{
		base: base,
	}
	for _, sl := range others {
		c.AddLayer(sl)
	}
	return c
}

func (c *Columns) Layers() int {
	return len(c.added)
}

func (c *Columns) Name(i int) string {
	return c.names[i]
}

// Swap swaps position of 2 columns
func (c *Columns) Swap(i, j int) {
	c.names[i], c.names[j] = c.names[j], c.names[i]
	for layer := 0; layer < c.Layers(); layer++ {
		for _, sl := range [][]map[int]struct{}{c.added, c.removed} {
			_, oki := sl[layer][i]
			_, okj := sl[layer][j]
			if oki && !okj {
				sl[layer][j] = struct{}{}
				delete(sl[layer], i)
			} else if okj && !oki {
				sl[layer][i] = struct{}{}
				delete(sl[layer], j)
			}
		}
		m := c.moved[layer]
		vi, oki := m[i]
		vj, okj := m[j]
		if oki && okj {
			m[i], m[j] = vj, vi
		} else if oki {
			m[j] = vi
			delete(m, i)
		} else if okj {
			m[i] = vj
			delete(m, j)
		}
	}
}

func (c *Columns) Names() []string {
	return c.names
}

func (c *Columns) Len() int {
	return len(c.names)
}

func (c *Columns) Added(i, j int) bool {
	_, ok := c.added[i][j]
	return ok
}

// Moved returns the index of "before" or "after" column that this column
// was moved away from. At most only one of them are greater than -1.
// if "before" is not -1 then this column was originally positioned
// before the column at "before" index. If "after" is not -1 then
// this column was originally positioned after the column at "after"
// index. If both are -1 then this column did not change position
// in regard to the columns surrounding it.
func (c *Columns) Moved(i, j int) (before int, after int) {
	v, ok := c.moved[i][j]
	if ok {
		return v[0], v[1]
	}
	return -1, -1
}

func (c *Columns) Removed(i, j int) bool {
	_, ok := c.removed[i][j]
	return ok
}

func (c *Columns) insertToNames(cols []string) {
	namesM := stringSliceToMap(c.names)
	n := len(c.names)
	var anchor = -1
	listM := map[int]*list.List{}
	total := 0
	for _, s := range cols {
		if i, ok := namesM[s]; ok {
			anchor = i
			continue
		}
		l, ok := listM[anchor]
		if !ok {
			listM[anchor] = list.New()
			l = listM[anchor]
		}
		l.PushBack(s)
		total++
	}
	if total == 0 {
		return
	}
	names := make([]string, n+total)
	prevAnchor := 0
	off := 0
	for anchor, l := range listM {
		copy(names[off:], c.names[prevAnchor:anchor+1])
		off += anchor + 1 - prevAnchor
		for e := l.Front(); e != nil; e = e.Next() {
			names[off] = e.Value.(string)
			off++
		}
		prevAnchor = anchor + 1
	}
	if prevAnchor < n {
		copy(names[off:], c.names[prevAnchor:])
	}
	c.names = names
}

func (c *Columns) AddLayer(cols []string) {
	layer := c.Layers()
	c.added = append(c.added, map[int]struct{}{})
	c.removed = append(c.removed, map[int]struct{}{})
	c.moved = append(c.moved, map[int][2]int{})

	c.insertToNames(cols)
	c.insertToNames(c.base)

	// populate added map
	baseM := stringSliceToMap(c.base)
	namesM := stringSliceToMap(c.names)
	for _, s := range cols {
		if _, ok := baseM[s]; !ok {
			c.added[layer][namesM[s]] = struct{}{}
		}
	}

	// populate removed map
	colsM := stringSliceToMap(cols)
	for _, s := range c.base {
		if _, ok := colsM[s]; !ok {
			c.removed[layer][namesM[s]] = struct{}{}
		}
	}

	c.populateMovedMap(layer, colsM)
}

func (c *Columns) populateMovedMap(layer int, colsM map[string]int) {
	common := []string{}
	for _, s := range c.base {
		if _, ok := colsM[s]; ok {
			common = append(common, s)
		}
	}
	commonM := stringSliceToMap(common)
	oldIndices := []int{}
	newIndices := []int{}
	for i, s := range c.names {
		if j, ok := commonM[s]; ok {
			newIndices = append(newIndices, i)
			oldIndices = append(oldIndices, j)
		}
	}
	ops := moveOps(oldIndices)
	nonAnchor := map[int]struct{}{}
	for _, v := range ops {
		nonAnchor[v.old] = struct{}{}
	}
	namesM := stringSliceToMap(c.names)
	for _, op := range ops {
		newIndex := newIndices[op.new]
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
			c.moved[layer][newIndex] = [2]int{-1, namesM[after]}
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
			c.moved[layer][newIndex] = [2]int{namesM[before], -1}
		}
	}
}

func (c *Columns) Less(i, j int) bool {
	if c.pk == nil {
		return i < j
	}
	vi, oki := c.pk[c.names[i]]
	vj, okj := c.pk[c.names[j]]
	if oki && okj {
		return vi < vj
	} else if oki {
		return true
	} else if okj {
		return false
	}
	return i < j
}

func (c *Columns) HoistPKToStart(pk []string) {
	c.pk = stringSliceToMap(pk)
	sort.Stable(c)
}
