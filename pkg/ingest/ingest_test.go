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

	sum, err := IngestTable(db, f, rows[0][:1])
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

func BenchmarkIngestTable(b *testing.B) {
	rows := testutils.BuildRawCSV(100, b.N)
	f := writeCSV(b, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()
	b.ResetTimer()
	_, err := IngestTable(db, f, rows[0][:1])
	require.NoError(b, err)
}
