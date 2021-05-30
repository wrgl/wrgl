// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func intSliceToMap(sl []int) map[int]struct{} {
	m := map[int]struct{}{}
	for _, i := range sl {
		m[i] = struct{}{}
	}
	return m
}

func TestColumns(t *testing.T) {
	for i, c := range []struct {
		base    []string
		cols    []string
		names   []string
		moved   map[int][2]int
		added   []int
		removed []int
	}{
		{
			base:  []string{"a"},
			cols:  []string{"a"},
			names: []string{"a"},
		},
		{
			base:    []string{"a"},
			cols:    []string{"b"},
			names:   []string{"a", "b"},
			added:   []int{1},
			removed: []int{0},
		},
		{
			base:  []string{"a", "b"},
			cols:  []string{"a", "b"},
			names: []string{"a", "b"},
		},
		{
			base:  []string{"a", "b"},
			cols:  []string{"b", "a"},
			names: []string{"b", "a"},
			moved: map[int][2]int{1: {0, -1}},
		},
		{
			base:    []string{"a", "b", "c"},
			cols:    []string{"c", "a"},
			names:   []string{"c", "a", "b"},
			moved:   map[int][2]int{1: {0, -1}},
			removed: []int{2},
		},
		{
			base:  []string{"c", "b", "a"},
			cols:  []string{"a", "b", "c"},
			names: []string{"a", "b", "c"},
			moved: map[int][2]int{0: {-1, 1}, 2: {1, -1}},
		},
		{
			base:  []string{"a", "b"},
			cols:  []string{"b", "c", "a"},
			names: []string{"b", "c", "a"},
			moved: map[int][2]int{2: {0, -1}},
			added: []int{1},
		},
		{
			base:  []string{"a", "d", "e", "b", "c"},
			cols:  []string{"a", "b", "c", "d", "e"},
			names: []string{"a", "b", "c", "d", "e"},
			moved: map[int][2]int{3: {-1, 0}, 4: {-1, 0}},
		},
		{
			base:    []string{"e", "b", "c", "d", "f"},
			cols:    []string{"a", "b", "c", "d", "e"},
			names:   []string{"a", "b", "c", "d", "f", "e"},
			moved:   map[int][2]int{5: {1, -1}},
			added:   []int{0},
			removed: []int{4},
		},
	} {
		obj := CompareColumns(c.base, c.cols)
		n := obj.Len()
		assert.Equal(t, len(c.names), n, "case %d", i)
		assert.Equal(t, c.names, obj.Names(), "case %d", i)

		addM := intSliceToMap(c.added)
		remM := intSliceToMap(c.removed)
		for j := 0; j < n; j++ {
			b, a := obj.Moved(0, j)
			if c.moved != nil {
				if p, ok := c.moved[j]; ok {
					assert.Equal(t, p, [2]int{b, a}, "case %d col %d", i, j)
					continue
				}
			}
			assert.Equal(t, [2]int{-1, -1}, [2]int{b, a}, "case %d col %d", i, j)

			added := obj.Added(0, j)
			if _, ok := addM[j]; ok {
				assert.True(t, added, "case %d col %d", i, j)
			} else {
				assert.False(t, added, "case %d col %d", i, j)
			}

			rem := obj.Removed(0, j)
			if _, ok := remM[j]; ok {
				assert.True(t, rem, "case %d col %d", i, j)
			} else {
				assert.False(t, rem, "case %d col %d", i, j)
			}
		}
	}
}

func TestColumnsSwap(t *testing.T) {
	c := CompareColumns([]string{"a", "b", "c"}, []string{"b", "a", "d"})
	assert.Equal(t, []string{"b", "c", "a", "d"}, c.Names())
	assert.True(t, c.Removed(0, 1))
	assert.True(t, c.Added(0, 3))
	b, a := c.Moved(0, 2)
	assert.Equal(t, []int{0, -1}, []int{b, a})

	c.Swap(0, 3)
	assert.Equal(t, []string{"d", "c", "a", "b"}, c.Names())
	assert.False(t, c.Added(0, 3))
	assert.True(t, c.Added(0, 0))

	c.Swap(1, 2)
	assert.Equal(t, []string{"d", "a", "c", "b"}, c.Names())
	assert.False(t, c.Removed(0, 1))
	assert.True(t, c.Removed(0, 2))
	b, a = c.Moved(0, 1)
	assert.Equal(t, []int{0, -1}, []int{b, a})
	b, a = c.Moved(0, 2)
	assert.Equal(t, []int{-1, -1}, []int{b, a})
}

func TestHoistPKToStart(t *testing.T) {
	c := CompareColumns([]string{"a", "b", "c"}, []string{"b", "a", "d"})
	assert.Equal(t, []string{"b", "c", "a", "d"}, c.Names())
	c.HoistPKToStart([]string{"d", "a"})
	assert.Equal(t, []string{"d", "a", "b", "c"}, c.Names())

	c = CompareColumns([]string{"a", "b", "c"}, []string{"a", "b", "c"})
	assert.Equal(t, []string{"a", "b", "c"}, c.Names())
	c.HoistPKToStart([]string{"a"})
	assert.Equal(t, []string{"a", "b", "c"}, c.Names())
}
