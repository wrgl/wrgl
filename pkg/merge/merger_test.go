// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	merge_testutils "github.com/wrgl/core/pkg/merge/testutils"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func hexToBytes(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func getTables(t *testing.T, db kv.DB, fs kv.FileStore, commits ...[]byte) (baseT table.Store, otherTs []table.Store) {
	base, err := versioning.SeekCommonAncestor(db, commits...)
	require.NoError(t, err)
	baseCom, err := versioning.GetCommit(db, base)
	require.NoError(t, err)
	baseT, err = table.ReadTable(db, fs, baseCom.Table)
	require.NoError(t, err)
	otherTs = make([]table.Store, len(commits))
	for i, sum := range commits {
		com, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		otherT, err := table.ReadTable(db, fs, com.Table)
		require.NoError(t, err)
		otherTs[i] = otherT
	}
	return
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
	collector := merge_testutils.CreateCollector(t, db, fs, baseCom)
	baseT, otherTs := getTables(t, db, fs, com1, com2)
	merger, err := merge.NewMerger(db, fs, collector, 0, baseT, otherTs...)
	require.NoError(t, err)

	merges := merge_testutils.CollectUnresolvedMerges(t, merger)
	assert.Equal(t, []*merge.Merge{
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

	rows := merge_testutils.CollectSortedRows(t, merger, nil)
	assert.Equal(t, [][]string{
		{"1", "e", "r"},
		{"3", "s", "d"},
		{"4", "r", "t"},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns(nil))
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
	collector := merge_testutils.CreateCollector(t, db, fs, baseCom)
	baseT, otherTs := getTables(t, db, fs, com1, com2)
	merger, err := merge.NewMerger(db, fs, collector, 0, baseT, otherTs...)
	require.NoError(t, err)

	merges := merge_testutils.CollectUnresolvedMerges(t, merger)
	assert.Equal(t, []*merge.Merge{
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
			ResolvedRow:    []string{"2", "a", "s"},
			UnresolvedCols: map[uint32]struct{}{2: {}},
		},
	}, merges)
	require.NoError(t, merger.Error())

	require.NoError(t, merger.SaveResolvedRow(
		hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"), nil,
	))
	require.NoError(t, merger.SaveResolvedRow(
		hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"), []string{"2", "c", "v"},
	))

	rows := merge_testutils.CollectSortedRows(t, merger, nil)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "c", "v"},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns(nil))
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}

func TestMergerRemoveCols(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	base, baseCom := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, fs, []string{
		"a,b",
		"1,q",
		"2,a",
		"4,r",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,e,w",
		"2,x,s",
		"4,f,t",
	}, []uint32{0}, [][]byte{base})
	collector := merge_testutils.CreateCollector(t, db, fs, baseCom)
	baseT, otherTs := getTables(t, db, fs, com1, com2)
	merger, err := merge.NewMerger(db, fs, collector, 0, baseT, otherTs...)
	require.NoError(t, err)

	merges := merge_testutils.CollectUnresolvedMerges(t, merger)
	assert.Equal(t, []*merge.Merge{
		{
			ColDiff: &objects.ColDiff{
				Names:   []string{"a", "b", "c"},
				BasePK:  []uint32{0},
				OtherPK: [][]uint32{{0}, {0}},
				Added:   []map[uint32]struct{}{{}, {}},
				Removed: []map[uint32]struct{}{{2: {}}, {}},
				Moved:   []map[uint32][]int{{}, {}},
				BaseIdx: map[uint32]uint32{0: 0, 1: 1, 2: 2},
				OtherIdx: []map[uint32]uint32{
					{0: 0, 1: 1},
					{0: 0, 1: 1, 2: 2},
				},
			},
		},
	}, merges)

	rows := merge_testutils.CollectSortedRows(t, merger, map[int]struct{}{2: {}})
	assert.Equal(t, [][]string{
		{"1", "e"},
		{"2", "x"},
		{"4", "f"},
	}, rows)
	assert.Equal(t, []string{"a", "b"}, merger.Columns(map[int]struct{}{2: {}}))
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}
