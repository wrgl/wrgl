// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestRowHashReader(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0

	pkHashes := createHashSlice(4)
	rowHashes := createHashSlice(4)

	builder := NewBuilder(db, fs, columns, pk, seed, 0)
	err := builder.InsertRow(0, pkHashes[0], rowHashes[0], []byte("a,b,c"))
	require.NoError(t, err)
	err = builder.InsertRow(2, pkHashes[1], rowHashes[1], []byte("d,e,f"))
	require.NoError(t, err)
	err = builder.InsertRow(3, pkHashes[2], rowHashes[2], []byte("g,h,j"))
	require.NoError(t, err)
	err = builder.InsertRow(1, pkHashes[3], rowHashes[3], []byte("l,m,n"))
	require.NoError(t, err)
	sum, err := builder.SaveTable()
	require.NoError(t, err)

	ts, err := ReadTable(db, fs, sum)
	require.NoError(t, err)

	for _, c := range []struct {
		offset int
		size   int
		rows   [][2][]byte
	}{
		{
			0, 2,
			[][2][]byte{
				{pkHashes[0], rowHashes[0]},
				{pkHashes[3], rowHashes[3]},
			},
		},
		{
			2, 2,
			[][2][]byte{
				{pkHashes[1], rowHashes[1]},
				{pkHashes[2], rowHashes[2]},
			},
		},
		{
			4, 2,
			[][2][]byte{},
		},
		{
			0, 0,
			[][2][]byte{
				{pkHashes[0], rowHashes[0]},
				{pkHashes[3], rowHashes[3]},
				{pkHashes[1], rowHashes[1]},
				{pkHashes[2], rowHashes[2]},
			},
		},
	} {
		rhr := ts.NewRowHashReader(c.offset, c.size)
		assert.Equal(t, c.rows, readAllRowHashes(t, rhr))
	}
}
