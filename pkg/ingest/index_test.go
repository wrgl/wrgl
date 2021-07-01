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

func getTableIndex(t *testing.T, db kvcommon.DB, sum []byte) [][]string {
	b, err := kv.GetTableIndex(db, sum)
	require.NoError(t, err)
	_, tblIdx, err := objects.ReadBlockFrom(bytes.NewReader(b))
	require.NoError(t, err)
	return tblIdx
}

func getBlockIndex(t *testing.T, db kvcommon.DB, sum []byte) *objects.BlockIndex {
	b, err := kv.GetBlockIndex(db, sum)
	require.NoError(t, err)
	_, idx, err := objects.ReadBlockIndex(bytes.NewReader(b))
	require.NoError(t, err)
	return idx
}

func TestIndexTable(t *testing.T) {
	rows := createRandomCSV([]string{"a", "b", "c", "d"}, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := kvtestutils.NewMockStore(false)

	sum, err := IngestTable(db, f, "test.csv", []string{"a"}, 0, 1, io.Discard)
	require.NoError(t, err)
	tbl := getTable(t, db, sum)
	tblIdx := getTableIndex(t, db, sum)
	require.NoError(t, kv.DeleteTableIndex(db, sum))
	blkIndices := []*objects.BlockIndex{}
	for _, sum := range tbl.Blocks {
		blkIndices = append(blkIndices, getBlockIndex(t, db, sum))
		require.NoError(t, kv.DeleteBlockIndex(db, sum))
	}

	require.NoError(t, IndexTable(db, sum, tbl))
	tblIdx2 := getTableIndex(t, db, sum)
	assert.Equal(t, tblIdx, tblIdx2)
	for i, sum := range tbl.Blocks {
		assert.Equal(t, blkIndices[i], getBlockIndex(t, db, sum))
	}
}
