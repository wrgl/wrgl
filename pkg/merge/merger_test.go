// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/merge"
	mergehelpers "github.com/wrgl/core/pkg/merge/helpers"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/sorter"
)

func hexToBytes(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestMergerAutoResolve(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,r",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,e,w",
		"3,s,d",
		"4,r,t",
	}, []uint32{0}, [][]byte{base})
	merger, _ := mergehelpers.CreateMerger(t, db, com1, com2)

	merges := mergehelpers.CollectUnresolvedMerges(t, merger)
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

	rows := mergehelpers.CollectSortedRows(t, merger, nil)
	assert.Equal(t, []*sorter.Block{
		{
			Block: [][]string{
				{"1", "e", "r"},
				{"3", "s", "d"},
				{"4", "r", "t"},
			},
			PK: []string{"1"},
		},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns(nil))
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}

func TestMergerManualResolve(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,s,d",
		"5,t,y",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,r",
		"3,x,d",
		"4,v,b",
		"5,t,y",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,w",
		"4,n,m",
		"5,t,u",
	}, []uint32{0}, [][]byte{base})
	merger, _ := mergehelpers.CreateMerger(t, db, com1, com2)

	merges := mergehelpers.CollectUnresolvedMerges(t, merger)
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
			PK: hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Others: [][]byte{
				hexToBytes(t, "924ab57f34c108531fa13fd94516b938"),
				hexToBytes(t, "d791f3fd29cfa068e41bc9c35e99cde9"),
			},
			BlockOffset:    []uint32{0, 0},
			RowOffset:      []byte{3, 2},
			ResolvedRow:    []string{"4", "", ""},
			UnresolvedCols: map[uint32]struct{}{1: {}, 2: {}},
		},
		{
			PK:            hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Base:          hexToBytes(t, "a07911e53273daff2622013f7d1d0ec9"),
			BaseRowOffset: 2,
			Others: [][]byte{
				hexToBytes(t, "5df6d1d1e8caf8fd5d67b5d264caace1"),
				nil,
			},
			BlockOffset: []uint32{0, 0},
			RowOffset:   []byte{2, 0},
			ResolvedRow: []string{"3", "x", "d"},
		},
		{
			PK:            hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
			Base:          hexToBytes(t, "e4f37424a61671456b0be328e4f3719c"),
			BaseRowOffset: 1,
			Others: [][]byte{
				hexToBytes(t, "fb93f68df361ea942678be1731936e32"),
				hexToBytes(t, "b573142d4d736d82e123239dc399cff1"),
			},
			BlockOffset:    []uint32{0, 0},
			RowOffset:      []byte{1, 1},
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
	require.NoError(t, merger.SaveResolvedRow(
		hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"), []string{"4", "n", "m"},
	))

	rows := mergehelpers.CollectSortedRows(t, merger, nil)
	assert.Equal(t, []*sorter.Block{
		{
			Block: [][]string{
				{"1", "q", "w"},
				{"2", "c", "v"},
				{"4", "n", "m"},
				{"5", "t", "u"},
			},
			PK: []string{"1"},
		},
	}, rows)
	assert.Equal(t, []string{"a", "b", "c"}, merger.Columns(nil))
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}

func TestMergerRemoveCols(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,r,t",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, []string{
		"a,b",
		"1,q",
		"2,a",
		"4,r",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,e,w",
		"2,x,s",
		"4,f,t",
	}, []uint32{0}, [][]byte{base})
	merger, _ := mergehelpers.CreateMerger(t, db, com1, com2)

	merges := mergehelpers.CollectUnresolvedMerges(t, merger)
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

	rows := mergehelpers.CollectSortedRows(t, merger, map[int]struct{}{2: {}})
	assert.Equal(t, []*sorter.Block{
		{
			Block: [][]string{
				{"1", "e"},
				{"2", "x"},
				{"4", "f"},
			},
			PK: []string{"1"},
		},
	}, rows)
	assert.Equal(t, []string{"a", "b"}, merger.Columns(map[int]struct{}{2: {}}))
	assert.Equal(t, []string{"a"}, merger.PK())
	require.NoError(t, merger.Close())
}
