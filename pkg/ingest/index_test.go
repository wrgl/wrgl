package ingest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestIndexTable(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())
	db := objmock.NewStore()
	s, err := sorter.NewSorter(0, nil)
	require.NoError(t, err)

	sum, err := IngestTable(db, s, f, rows[0][:1])
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	tblIdx, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	require.NoError(t, objects.DeleteTableIndex(db, sum))
	blkIndices := []*objects.BlockIndex{}
	var idx *objects.BlockIndex
	var bb []byte
	for _, sum := range tbl.BlockIndices {
		idx, bb, err = objects.GetBlockIndex(db, bb, sum)
		require.NoError(t, err)
		blkIndices = append(blkIndices, idx)
		require.NoError(t, objects.DeleteBlockIndex(db, sum))
	}

	require.NoError(t, IndexTable(db, sum, tbl, nil))
	tblIdx2, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, tblIdx, tblIdx2)
	for i, sum := range tbl.BlockIndices {
		idx, bb, err = objects.GetBlockIndex(db, bb, sum)
		require.NoError(t, err)
		assert.Equal(t, blkIndices[i], idx)
	}
}
