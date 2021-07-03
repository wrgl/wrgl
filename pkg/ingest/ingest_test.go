package ingest

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/sorter"
	"github.com/wrgl/core/pkg/testutils"
)

func writeCSV(t *testing.T, rows [][]string) *os.File {
	t.Helper()
	f, err := ioutil.TempFile("", "*.csv")
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

	sum, err := IngestTable(db, f, f.Name(), rows[0][:1], 0, 1, io.Discard)
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

	sorter.SortBlock(rows[1:], []uint32{0})
	for i, sum := range tbl.Blocks {
		blk, err := objects.GetBlock(db, sum)
		require.NoError(t, err)
		for j, row := range blk {
			if j == 0 {
				assert.Equal(t, tblIdx[i], row[:1])
			}
			assert.Equal(t, rows[i*255+j+1], row)
		}

		blkIdx, err := objects.GetBlockIndex(db, sum)
		require.NoError(t, err)
		assert.Len(t, blkIdx.Rows, len(blk))
	}
}
