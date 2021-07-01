package ingest

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/objects"
)

func getTable(t *testing.T, db kvcommon.DB, sum []byte) *objects.Table {
	b, err := kv.GetTable(db, sum)
	require.NoError(t, err)
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	require.NoError(t, err)
	return tbl
}

func TestIngestTable(t *testing.T) {
	rows := createRandomCSV([]string{"a", "b", "c", "d"}, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := kvtestutils.NewMockStore(false)

	sum, err := IngestTable(db, f, f.Name(), []string{"a"}, 0, 1, io.Discard)
	require.NoError(t, err)

	tbl := getTable(t, db, sum)
	assert.Equal(t, []string{"a", "b", "c", "d"}, tbl.Columns)
	assert.Equal(t, []uint32{0}, tbl.PK)
	assert.Equal(t, 700, int(tbl.RowsCount))
	assert.Len(t, tbl.Blocks, 3)

	tblIdx := getTableIndex(t, db, sum)
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
