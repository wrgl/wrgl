// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"testing"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
)

func addRow(t *testing.T, db kv.DB, row []string) []byte {
	t.Helper()
	enc := objects.NewStrListEncoder()
	b := enc.Encode(row)
	sum := meow.Checksum(0, b)
	err := table.SaveRow(db, sum[:], b)
	require.NoError(t, err)
	return sum[:]
}

func addRandomRow(t *testing.T, db kv.DB, n int) ([]byte, []string) {
	t.Helper()
	row := []string{}
	for i := 0; i < n; i++ {
		row = append(row, testutils.BrokenRandomAlphaNumericString(5))
	}
	sum := addRow(t, db, row)
	return sum, row
}

func TestResolver(t *testing.T) {
	db := kv.NewMockStore(false)
	cd := objects.CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [][2][]string{
		{{"a", "b", "c"}, {"a"}},
		{{"a", "b", "c"}, {"a"}},
	}...)
	r := NewResolver(db, cd)

	// resolve removed row
	m := &Merge{
		Base:   testutils.SecureRandomBytes(16),
		Others: make([][]byte, 2),
	}
	require.NoError(t, r.Resolve(m))
	assert.True(t, m.Resolved)
	assert.Nil(t, m.ResolvedRow)

	// won't resolve modified & removed row
	m = &Merge{
		Base:   testutils.SecureRandomBytes(16),
		Others: [][]byte{nil, testutils.SecureRandomBytes(16)},
	}
	require.NoError(t, r.Resolve(m))
	assert.False(t, m.Resolved)
	assert.Nil(t, m.ResolvedRow)

	// resolve singly added row
	sum, row := addRandomRow(t, db, 3)
	m = &Merge{
		Others: [][]byte{sum, nil},
	}
	require.NoError(t, r.Resolve(m))
	assert.True(t, m.Resolved)
	assert.Equal(t, row, m.ResolvedRow)

	// resolve similarly modified row
	sum, row = addRandomRow(t, db, 3)
	m = &Merge{
		Others: [][]byte{sum, sum},
	}
	require.NoError(t, r.Resolve(m))
	assert.True(t, m.Resolved)
	assert.Equal(t, row, m.ResolvedRow)

	// resolve similarly modified row
	sum, row = addRandomRow(t, db, 3)
	m = &Merge{
		Base:   testutils.SecureRandomBytes(16),
		Others: [][]byte{sum, sum},
	}
	require.NoError(t, r.Resolve(m))
	assert.True(t, m.Resolved)
	assert.Equal(t, row, m.ResolvedRow)
}

func TestResolveRow(t *testing.T) {
	cd1 := objects.CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [][2][]string{
		{{"a", "b", "c", "d"}, {"a"}},
		{{"a", "b"}, {"a"}},
	}...)
	cd2 := objects.CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [][2][]string{
		{{"a", "b"}, {"a"}},
		{{"a", "b", "c", "d"}, {"a"}},
	}...)
	cd3 := objects.CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [][2][]string{
		{{"a", "b", "c", "d"}, {"a"}},
		{{"a", "b", "c", "d"}, {"a"}},
	}...)

	for i, c := range []struct {
		cd          *objects.ColDiff
		base        []string
		others      [][]string
		resolved    bool
		resolvedRow []string
	}{
		{
			cd:   cd1,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "w", "e", "r"},
				{"q", "w"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "w", "", "r"},
		},
		{
			cd:   cd2,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "w"},
				{"q", "w", "e", "r"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "w", "", "r"},
		},
		{
			cd:   cd1,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "s", "e", "r"},
				{"q", "s"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "s", "", "r"},
		},
		{
			cd:   cd2,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "s"},
				{"q", "s", "e", "r"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "s", "", "r"},
		},
		{
			cd:   cd1,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "w", "y", "r"},
				{"q", "w"},
			},
		},
		{
			cd:   cd2,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "w"},
				{"q", "w", "y", "r"},
			},
		},
		{
			cd:   cd1,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "u", "e", "r"},
				{"q", "s"},
			},
		},
		{
			cd:   cd2,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "s"},
				{"q", "u", "e", "r"},
			},
		},
		{
			cd:   cd1,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "w", "e", "r"},
				{"q", "u"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "u", "", "r"},
		},
		{
			cd:   cd2,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "u"},
				{"q", "w", "e", "r"},
			},
			resolved:    true,
			resolvedRow: []string{"q", "u", "", "r"},
		},
		{
			cd:   cd3,
			base: []string{"q", "w", "e"},
			others: [][]string{
				{"q", "u", "e", "g"},
				{"q", "u", "e", "r"},
			},
		},
	} {
		db := kv.NewMockStore(false)
		r := NewResolver(db, c.cd)
		m := &Merge{
			Base: addRow(t, db, c.base),
			Others: [][]byte{
				addRow(t, db, c.others[0]),
				addRow(t, db, c.others[1]),
			},
		}
		require.NoError(t, r.Resolve(m))
		assert.Equal(t, c.resolved, m.Resolved, "case %d", i)
		assert.Equal(t, c.resolvedRow, m.ResolvedRow, "case %d", i)
	}
}
