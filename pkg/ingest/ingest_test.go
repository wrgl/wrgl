package ingest

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/objects"
)

func TestIngestTable(t *testing.T) {
	rows := createRandomCSV([]string{"a", "b", "c", "d"}, 700)
	name := writeCSV(t, rows)
	defer os.Remove(name)
	db := kvtestutils.NewMockStore(false)

	sum, err := IngestTable(db, name, []string{"a"}, 0, 1, io.Discard)
	require.NoError(t, err)

	b, err := kv.GetTable(db, sum)
	require.NoError(t, err)
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, tbl.Columns)
	assert.Equal(t, []uint32{0}, tbl.PK)
	assert.Equal(t, 700, int(tbl.RowsCount))
	assert.Len(t, tbl.Blocks, 3)

	b, err = kv.GetTableIndex(db, sum)
	require.NoError(t, err)
	_, tblIdx, err := objects.ReadBlockFrom(bytes.NewReader(b))
	require.NoError(t, err)
	assert.Len(t, tblIdx, 3)

	sortBlock(rows[1:], []uint32{0})
	for i, sum := range tbl.Blocks {
		b, err := kv.GetBlock(db, sum)
		require.NoError(t, err)
		_, blk, err := objects.ReadBlockFrom(bytes.NewReader(b))
		require.NoError(t, err)
		for j, row := range blk {
			if j == 0 {
				assert.Equal(t, tblIdx[i], row[:1])
			}
			assert.Equal(t, rows[i*255+j+1], row)
		}

		b, err = kv.GetBlockIndex(db, sum)
		require.NoError(t, err)
		_, blkIdx, err := objects.ReadBlockIndex(bytes.NewReader(b))
		require.NoError(t, err)
		assert.Len(t, blkIdx.Rows, len(blk))
	}
}
