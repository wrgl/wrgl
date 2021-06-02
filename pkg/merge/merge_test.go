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
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

func hexToBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestMergeCommit(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	base, _ := factory.Commit(t, db, fs, []string{
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
	errCh := make(chan error, 100)

	mergeCh, _, err := MergeCommits(db, fs, 0, errCh, base, com1, com2)
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
			PK: hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Others: [][]byte{
				nil,
				hexToBytes(t, "a07911e53273daff2622013f7d1d0ec9"),
			},
			ResolvedRow: []string{"3", "s", "d"},
			Resolved:    true,
		},
		{
			PK:   hexToBytes(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Base: hexToBytes(t, "60f1c744d65482e468bfac458a7131fe"),
			Others: [][]byte{
				hexToBytes(t, "ad8fb5da435d04ee83f91bc21ba54059"),
				hexToBytes(t, "66fa86312e51a3684e890619c871a63b"),
			},
			Resolved:    true,
			ResolvedRow: []string{"1", "e", "r"},
		},
		{
			PK:       hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
			Base:     hexToBytes(t, "e4f37424a61671456b0be328e4f3719c"),
			Others:   [][]byte{nil, nil},
			Resolved: true,
		},
	}, merges)

	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
}
