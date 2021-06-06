// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/index"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

func createCollector(t *testing.T, db kv.DB, fs kv.FileStore, baseCom *objects.Commit) *RowCollector {
	t.Helper()
	resolvedRows, err := NewSortableRows(misc.NewBuffer(nil), misc.NewBuffer(nil), []int{1})
	require.NoError(t, err)
	discardedRows, err := index.NewHashSet(misc.NewBuffer(nil), 0)
	require.NoError(t, err)
	baseT, err := table.ReadTable(db, fs, baseCom.Table)
	require.NoError(t, err)
	collector, err := NewCollector(db, baseT, resolvedRows, discardedRows)
	require.NoError(t, err)
	return collector
}

func collectUnresolvedMerges(t *testing.T, merger *Merger) []Merge {
	t.Helper()
	mergeCh, err := merger.Start()
	require.NoError(t, err)
	merges := []Merge{}
	for m := range mergeCh {
		merges = append(merges, m)
	}
	sort.SliceStable(merges, func(i, j int) bool {
		if merges[i].ColDiff != nil && merges[j].ColDiff == nil {
			return true
		}
		if merges[j].ColDiff != nil && merges[i].ColDiff == nil {
			return false
		}
		if merges[i].Base == nil && merges[j].Base != nil {
			return true
		}
		if merges[j].Base == nil && merges[i].Base != nil {
			return false
		}
		return string(merges[i].Base) < string(merges[j].Base)
	})
	return merges
}

func collectSortedRows(t *testing.T, merger *Merger) [][]string {
	t.Helper()
	rows := [][]string{}
	ch, err := merger.SortedRows()
	require.NoError(t, err)
	for sl := range ch {
		rows = append(rows, sl)
	}
	require.NoError(t, merger.Error())
	return rows
}

func TestMergeCommit(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	base, baseCom := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,r",
		"2,a,s",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,e,w",
		"3,s,d",
	}, []uint32{0}, [][]byte{base})
	collector := createCollector(t, db, fs, baseCom)
	merger, err := NewMerger(db, fs, collector, 0, com1, com2)
	require.NoError(t, err)

	merges := collectUnresolvedMerges(t, merger)
	assert.Equal(t, []Merge{
		{
			ColDiff: &objects.ColDiff{
				Names:   []string{"a", "b", "c"},
				BasePK:  []uint32{0},
				OtherPK: [][]uint32{{0}, {0}},
				Added:   []map[uint32]struct{}{{}, {}},
				Removed: []map[uint32]struct{}{{}, {}},
				Moved:   []map[uint32][]int{{}, {}},
				BaseIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2},
				OtherIdx: []map[uint32]uint32{
					{0: 0, 1: 1, 2: 2},
					{0: 0, 1: 1, 2: 2},
				},
			},
		},
	}, merges)
	require.NoError(t, merger.Error())

	rows := collectSortedRows(t, merger)
	assert.Equal(t, [][]string{
		{"1", "e", "r"},
		{"3", "s", "d"},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns())
	assert.Equal(t, []string{"a"}, merger.PK())
}
