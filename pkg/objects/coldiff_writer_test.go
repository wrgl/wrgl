// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColDiffWriter(t *testing.T) {
	for i, c := range []struct {
		ColDiff *ColDiff
	}{
		{&ColDiff{
			Names:    []string{"a", "b", "c", "d", "f", "e"},
			BasePK:   []uint32{1},
			OtherPK:  [][]uint32{{0, 1}},
			Moved:    []map[uint32][]int{{5: {1, -1}}},
			Added:    []map[uint32]struct{}{{0: struct{}{}}},
			Removed:  []map[uint32]struct{}{{4: struct{}{}}},
			BaseIdx:  map[uint32]uint32{1: 1, 2: 2, 3: 3, 4: 4, 5: 0},
			OtherIdx: []map[uint32]uint32{{0: 0, 1: 1, 2: 2, 3: 3, 5: 4}},
		}},
		{&ColDiff{
			Names:   []string{"a", "b", "c", "d", "f", "e"},
			BasePK:  []uint32{4},
			OtherPK: [][]uint32{{0}, nil},
			Moved:   []map[uint32][]int{{5: {1, -1}}, {3: {-1, 0}}},
			Added:   []map[uint32]struct{}{{0: struct{}{}}, {1: struct{}{}, 3: struct{}{}}},
			Removed: []map[uint32]struct{}{{4: struct{}{}}, {}},
			BaseIdx: map[uint32]uint32{1: 1, 2: 2, 3: 3, 4: 4, 5: 0},
			OtherIdx: []map[uint32]uint32{
				{0: 0, 1: 1, 2: 2, 3: 3, 5: 4},
				{1: 0, 3: 1, 2: 2, 5: 3},
			},
		}},
	} {
		b, err := EncodeColDiff(c.ColDiff)
		require.NoError(t, err)
		r := NewColDiffReader(bytes.NewReader(b))
		_, cd, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, c.ColDiff, cd, "case %d", i)
	}
}
