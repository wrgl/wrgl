// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package merge_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/merge"
	mergehelpers "github.com/wrgl/wrgl/pkg/merge/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
)

func TestRowResolverSimpleCases(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"4,e,r",
	}, []uint32{0}, nil)
	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"2,a,s",
		"3,z,x",
		"4,e,t",
	}, []uint32{0}, [][]byte{base})
	sum2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"3,z,x",
		"4,e,t",
	}, []uint32{0}, [][]byte{base})

	m, _ := mergehelpers.CreateMerger(t, db, sum1, sum2)
	merges := mergehelpers.CollectUnresolvedMerges(t, m)
	assert.Len(t, merges, 1)
	assert.NotEmpty(t, merges[0].ColDiff)
	blocks := mergehelpers.CollectSortedRows(t, m, nil)
	assert.Equal(t, []*sorter.Rows{
		{
			Rows: [][]string{
				{"2", "a", "s"},
				{"3", "z", "x"},
				{"4", "e", "t"},
			},
		},
	}, blocks)
}

func TestRowResolverComplexCases(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
		"4,t,y",
		"5,g,h",
		"6,b,n",
	}, []uint32{0}, nil)
	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c,d",
		"1,q,w,e",
		"2,g,s,d",
		"3,z,v,c",
		"4,q,y,u",
		"5,g,h,j",
		"6,m,n,k",
	}, []uint32{0}, [][]byte{base})
	sum2, _ := factory.Commit(t, db, []string{
		"a,b",
		"1,q",
		"2,g",
		"3,z",
		"4,a",
		"5,s",
	}, []uint32{0}, [][]byte{base})

	m, _ := mergehelpers.CreateMerger(t, db, sum1, sum2)
	merges := mergehelpers.CollectUnresolvedMerges(t, m)
	assert.Equal(t, []*merge.Merge{
		{
			PK:         hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Base:       hexToBytes(t, "1c51f6044190122c554cc6794585e654"),
			BaseOffset: 2,
			Others: [][]byte{
				hexToBytes(t, "c0862c2d8d7f0bf7bc7bbb0890497f6a"),
				hexToBytes(t, "776beabc377528a964029835c5387e86"),
			},
			OtherOffsets:   []uint32{2, 2},
			ResolvedRow:    []string{"3", "z", "x", "c"},
			UnresolvedCols: map[uint32]struct{}{2: {}},
		},
		{
			PK:         hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Base:       hexToBytes(t, "9896effbd1a3352e214a496218523c12"),
			BaseOffset: 3,
			Others: [][]byte{
				hexToBytes(t, "da8e1ab26a4ee16559d154a16b380648"),
				hexToBytes(t, "85785beedceb27a5a18d7facd8ab23be"),
			},
			OtherOffsets:   []uint32{3, 3},
			ResolvedRow:    []string{"4", "t", "", "u"},
			UnresolvedCols: map[uint32]struct{}{1: {}},
		},
		{
			PK:         hexToBytes(t, "6a9a95a94e30ba3442e56419c04c259e"),
			Base:       hexToBytes(t, "b36317b30e927db6160d5fc158c509f8"),
			BaseOffset: 5,
			Others: [][]byte{
				hexToBytes(t, "00a35d4d1ffc6af8c8f8832555ae7ebb"),
				nil,
			},
			OtherOffsets:   []uint32{5, 0},
			ResolvedRow:    []string{"6", "b", "n", "k"},
			UnresolvedCols: map[uint32]struct{}{1: {}},
		},
	}, merges[1:])
	blocks := mergehelpers.CollectSortedRows(t, m, nil)
	assert.Equal(t, []*sorter.Rows{
		{
			Rows: [][]string{
				{"1", "q", "", "e"},
				{"2", "g", "", "d"},
				{"3", "z", "x"},
				{"4", "t", "y"},
				{"5", "s", "", "j"},
				{"6", "b", "n"},
			},
		},
	}, blocks)

	m, _ = mergehelpers.CreateMerger(t, db, sum2, sum1)
	merges = mergehelpers.CollectUnresolvedMerges(t, m)
	assert.Equal(t, []*merge.Merge{
		{
			PK:         hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Base:       hexToBytes(t, "1c51f6044190122c554cc6794585e654"),
			BaseOffset: 2,
			Others: [][]byte{
				hexToBytes(t, "776beabc377528a964029835c5387e86"),
				hexToBytes(t, "c0862c2d8d7f0bf7bc7bbb0890497f6a"),
			},
			OtherOffsets:   []uint32{2, 2},
			ResolvedRow:    []string{"3", "z", "x", "c"},
			UnresolvedCols: map[uint32]struct{}{2: {}},
		},
		{
			PK:         hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Base:       hexToBytes(t, "9896effbd1a3352e214a496218523c12"),
			BaseOffset: 3,
			Others: [][]byte{
				hexToBytes(t, "85785beedceb27a5a18d7facd8ab23be"),
				hexToBytes(t, "da8e1ab26a4ee16559d154a16b380648"),
			},
			OtherOffsets:   []uint32{3, 3},
			ResolvedRow:    []string{"4", "t", "", "u"},
			UnresolvedCols: map[uint32]struct{}{1: {}},
		},
		{
			PK:         hexToBytes(t, "6a9a95a94e30ba3442e56419c04c259e"),
			Base:       hexToBytes(t, "b36317b30e927db6160d5fc158c509f8"),
			BaseOffset: 5,
			Others: [][]byte{
				nil,
				hexToBytes(t, "00a35d4d1ffc6af8c8f8832555ae7ebb"),
			},
			OtherOffsets:   []uint32{0, 5},
			ResolvedRow:    []string{"6", "b", "n", "k"},
			UnresolvedCols: map[uint32]struct{}{1: {}},
		},
	}, merges[1:])
	blocks = mergehelpers.CollectSortedRows(t, m, nil)
	assert.Equal(t, []*sorter.Rows{
		{
			Rows: [][]string{
				{"1", "q", "", "e"},
				{"2", "g", "", "d"},
				{"3", "z", "x"},
				{"4", "t", "y"},
				{"5", "s", "", "j"},
				{"6", "b", "n"},
			},
		},
	}, blocks)

	base, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"3,z,x",
		"4,r,t",
	}, []uint32{0}, nil)
	sum1, _ = factory.Commit(t, db, []string{
		"a,b,c,d",
		"1,y,w,u",
		"2,a,s,f",
	}, []uint32{0}, [][]byte{base})
	sum2, _ = factory.Commit(t, db, []string{
		"a,b,c,d",
		"1,y,w,t",
		"2,a,s,d",
		"3,v,x,c",
		"4,r,t,y",
	}, []uint32{0}, [][]byte{base})

	m, _ = mergehelpers.CreateMerger(t, db, sum1, sum2)
	merges = mergehelpers.CollectUnresolvedMerges(t, m)
	assert.Equal(t, []*merge.Merge{
		{
			PK: hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
			Others: [][]byte{
				hexToBytes(t, "16cf50d440fc0422278abb626446d3e9"),
				hexToBytes(t, "4740760d0aaeecd7cac6ee3eb423ecea"),
			},
			OtherOffsets:   []uint32{1, 1},
			ResolvedRow:    []string{"2", "a", "s", ""},
			UnresolvedCols: map[uint32]struct{}{3: {}},
		},
		{
			PK:         hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Base:       hexToBytes(t, "1c51f6044190122c554cc6794585e654"),
			BaseOffset: 1,
			Others: [][]byte{
				nil,
				hexToBytes(t, "28c710dac52b757b8626ecb45fd5cf8b"),
			},
			OtherOffsets:   []uint32{0, 2},
			ResolvedRow:    []string{"3", "z", "x", "c"},
			UnresolvedCols: map[uint32]struct{}{1: {}},
		},
		{
			PK:   hexToBytes(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Base: hexToBytes(t, "60f1c744d65482e468bfac458a7131fe"),
			Others: [][]byte{
				hexToBytes(t, "3ae9ce5c2ac6dce8c1e92dc4a6ab7b2c"),
				hexToBytes(t, "114ee66177b00886476ebe85b13973a9"),
			},
			OtherOffsets:   []uint32{0, 0},
			ResolvedRow:    []string{"1", "y", "w", ""},
			UnresolvedCols: map[uint32]struct{}{3: {}},
		},
		{
			PK:         hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Base:       hexToBytes(t, "ba2ba1572859a3a224ad5d611cdc3528"),
			BaseOffset: 2,
			Others: [][]byte{
				nil,
				hexToBytes(t, "bc6f5a2ee80c7a0efa18fe22ee76a491"),
			},
			OtherOffsets: []uint32{0, 3},
			ResolvedRow:  []string{"4", "r", "t", "y"},
		},
	}, merges[1:])
	blocks = mergehelpers.CollectSortedRows(t, m, nil)
	assert.Equal(t, []*sorter.Rows{
		{
			Rows: [][]string{
				{"1", "q", "w"},
				{"3", "z", "x"},
				{"4", "r", "t"},
			},
		},
	}, blocks)
}
