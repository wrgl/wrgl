package ingest

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
)

func TestIngestTable(t *testing.T) {
	rows := createRandomCSV([]string{"a", "b", "c", "d"}, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()

	sum, err := IngestTable(db, f, f.Name(), []string{"a"}, 0, 1, io.Discard)
	require.NoError(t, err)

	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, tbl.Columns)
	assert.Equal(t, []uint32{0}, tbl.PK)
	assert.Equal(t, 700, int(tbl.RowsCount))
	assert.Len(t, tbl.Blocks, 3)

	tblIdx, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Len(t, tblIdx, 3)

	sortBlock(rows[1:], []uint32{0})
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
