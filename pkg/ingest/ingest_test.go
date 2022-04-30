// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ingest

import (
	"encoding/csv"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func writeCSV(t testing.TB, rows [][]string) *os.File {
	t.Helper()
	f, err := testutils.TempFile("", "*.csv")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return f
}

func TestIngestTable(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()
	s, err := sorter.NewSorter()
	require.NoError(t, err)

	sum, err := IngestTable(db, s, f, rows[0][:1])
	require.NoError(t, err)

	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	assert.Equal(t, rows[0], tbl.Columns)
	assert.Equal(t, []uint32{0}, tbl.PK)
	assert.Equal(t, 700, int(tbl.RowsCount))
	assert.Len(t, tbl.Blocks, 3)

	tblIdx, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Len(t, tblIdx, 3)

	tblProf, err := objects.GetTableProfile(db, sum)
	require.NoError(t, err)
	assert.Equal(t, uint32(700), tblProf.RowsCount)
	assert.Len(t, tblProf.Columns, 4)

	sorter.SortRows(rows[1:], []uint32{0})
	var bb []byte
	var blk [][]string
	var blkIdx *objects.BlockIndex
	for i, sum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		for j, row := range blk {
			if j == 0 {
				assert.Equal(t, tblIdx[i], row[:1])
			}
			assert.Equal(t, rows[i*255+j+1], row)
		}

		blkIdx, bb, err = objects.GetBlockIndex(db, bb, tbl.BlockIndices[i])
		require.NoError(t, err)
		assert.Len(t, blkIdx.Rows, len(blk))
	}
}

func TestRejectEmptyColumnName(t *testing.T) {
	rows := [][]string{
		{"", "a", "b"},
		{"1", "q", "w"},
		{"2", "a", "s"},
	}
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()
	s, err := sorter.NewSorter()
	require.NoError(t, err)

	_, err = IngestTable(db, s, f, rows[0][1:2])
	assert.Equal(t, `column name at position 0 is empty`, err.Error())

	_, err = IngestTableFromBlocks(db, s, []string{"", "a", "b"}, []uint32{1}, 2, make(<-chan *sorter.Block))
	assert.Equal(t, `column name at position 0 is empty`, err.Error())
}

func BenchmarkIngestTable(b *testing.B) {
	rows := testutils.BuildRawCSV(100, b.N)
	f := writeCSV(b, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()
	s, err := sorter.NewSorter()
	require.NoError(b, err)
	b.ResetTimer()
	_, err = IngestTable(db, s, f, rows[0][:1])
	require.NoError(b, err)
}
