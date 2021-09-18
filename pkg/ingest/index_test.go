package ingest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestIndexTable(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()

	sum, err := IngestTable(db, f, rows[0][:1])
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	tblIdx, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	require.NoError(t, objects.DeleteTableIndex(db, sum))
	blkIndices := []*objects.BlockIndex{}
	for _, sum := range tbl.Blocks {
		idx, err := objects.GetBlockIndex(db, sum)
		require.NoError(t, err)
		blkIndices = append(blkIndices, idx)
		require.NoError(t, objects.DeleteBlockIndex(db, sum))
	}

	require.NoError(t, IndexTable(db, sum, tbl, nil))
	tblIdx2, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, tblIdx, tblIdx2)
	for i, sum := range tbl.Blocks {
		idx, err := objects.GetBlockIndex(db, sum)
		require.NoError(t, err)
		assert.Equal(t, blkIndices[i], idx)
	}
}
