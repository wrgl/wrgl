// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"encoding/hex"
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

func hexToBytes(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

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

func TestMergerAutoResolve(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	base, baseCom := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,r",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,e,w",
		"3,s,d",
		"4,r,t",
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
		{"4", "r", "t"},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns())
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}

func TestMergerManualResolve(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	base, baseCom := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,s,d",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,r",
		"3,x,d",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,w",
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
		{
			PK:   hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Base: hexToBytes(t, "a07911e53273daff2622013f7d1d0ec9"),
			Others: [][]byte{
				hexToBytes(t, "5df6d1d1e8caf8fd5d67b5d264caace1"),
				nil,
			},
		},
		{
			PK:   hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
			Base: hexToBytes(t, "e4f37424a61671456b0be328e4f3719c"),
			Others: [][]byte{
				hexToBytes(t, "fb93f68df361ea942678be1731936e32"),
				hexToBytes(t, "b573142d4d736d82e123239dc399cff1"),
			},
		},
	}, merges)
	require.NoError(t, merger.Error())

	require.NoError(t, merger.SaveResolvedRow(
		hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"), nil,
	))
	require.NoError(t, merger.SaveResolvedRow(
		hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"), []string{"2", "c", "v"},
	))

	rows := collectSortedRows(t, merger)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "c", "v"},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns())
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}
