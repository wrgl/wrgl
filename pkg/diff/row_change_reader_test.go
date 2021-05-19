// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"encoding/hex"
	"io"
	"testing"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

func insertRecord(t *testing.T, db kv.DB, row []string) string {
	t.Helper()
	enc := objects.NewStrListEncoder()
	b := enc.Encode(row)
	sumArr := meow.Checksum(0, b)
	err := table.SaveRow(db, sumArr[:], b)
	require.NoError(t, err)
	return hex.EncodeToString(sumArr[:])
}

func TestRowChangeReader(t *testing.T) {
	db := kv.NewMockStore(false)
	cols := []string{"a", "b", "c", "d"}
	oldCols := []string{"a", "b", "c", "e"}
	pk := []string{"a"}

	sumPairs := [][2]string{}
	for _, recs := range [][2][]string{
		{
			[]string{"1", "2", "3", "4"},
			[]string{"1", "2", "3", "5"},
		},
		{
			[]string{"2", "2", "3", "4"},
			[]string{"2", "6", "3", "5"},
		},
		{
			[]string{"3", "2", "7", "4"},
			[]string{"3", "2", "3", "5"},
		},
	} {
		sumPairs = append(sumPairs, [2]string{
			insertRecord(t, db, recs[0]),
			insertRecord(t, db, recs[1]),
		})
	}

	// test Read
	reader, err := NewRowChangeReader(db, db, cols, oldCols, pk)
	require.NoError(t, err)
	reader.AddRowPair(sumPairs[0][0], sumPairs[0][1])
	assert.Equal(t, 1, reader.NumRows())
	mr, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"5"}, {"4"},
	}, mr)

	// test seek
	reader.AddRowPair(sumPairs[1][0], sumPairs[1][1])
	reader.AddRowPair(sumPairs[2][0], sumPairs[2][1])
	assert.Equal(t, 3, reader.NumRows())
	reader.Seek(1, io.SeekCurrent)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"3"}, {"2"}, {"7", "3"}, {"5"}, {"4"},
	}, mr)
	reader.Seek(-2, io.SeekEnd)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"2"}, {"2", "6"}, {"3"}, {"5"}, {"4"},
	}, mr)
	reader.Seek(0, io.SeekStart)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"5"}, {"4"},
	}, mr)

	// test readAt
	mr, err = reader.ReadAt(1)
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"2"}, {"2", "6"}, {"3"}, {"5"}, {"4"},
	}, mr)
}
