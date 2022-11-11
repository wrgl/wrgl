package ingest

import (
	"bytes"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/pckhoi/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
)

func TestReingest(t *testing.T) {
	db := objmock.NewStore()
	s, err := sorter.NewSorter()
	require.NoError(t, err)
	defer s.Close()
	hash := meow.New(0)
	enc := objects.NewStrListEncoder(true)
	buf := bytes.NewBuffer(nil)
	bb := []byte{}
	cols := []string{"q", "w", "e"}
	pk := []uint32{0}
	logger := testr.New(t)

	// save block
	blk := [][]string{
		{"1", "a", "b"},
		{"2", "c", "d"},
		{"2", "c", "d"},
		{"3", "e", "f"},
	}
	buf.Reset()
	_, err = objects.WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	blkSum, bb, err := objects.SaveBlock(db, bb, buf.Bytes())
	require.NoError(t, err)

	// save block index
	idx, err := objects.IndexBlock(enc, hash, blk, pk)
	require.NoError(t, err)
	buf.Reset()
	_, err = idx.WriteTo(buf)
	require.NoError(t, err)
	blkIdxSum, _, err := objects.SaveBlockIndex(db, bb, buf.Bytes())
	require.NoError(t, err)

	for _, useBlockIndex := range []bool{true, false} {
		tbl := &objects.Table{
			Columns:      cols,
			PK:           pk,
			RowsCount:    uint32(len(blk)),
			Blocks:       [][]byte{blkSum},
			BlockIndices: [][]byte{blkIdxSum},
		}
		newSum, err := ReingestTable(db, s, tbl, useBlockIndex, logger)
		require.NoError(t, err)
		newTbl, err := objects.GetTable(db, newSum)
		require.NoError(t, err)
		assert.Equal(t, tbl.Columns, newTbl.Columns)
		assert.Equal(t, tbl.PK, newTbl.PK)
		assert.Equal(t, tbl.RowsCount-1, newTbl.RowsCount)
		assert.Len(t, newTbl.Blocks, 1)
		assert.Len(t, newTbl.BlockIndices, 1)
		assert.NotEqual(t, tbl.Blocks[0], newTbl.Blocks[0])
		assert.NotEqual(t, tbl.BlockIndices[0], newTbl.BlockIndices[0])
		newBlk, _, err := objects.GetBlock(db, bb, newTbl.Blocks[0])
		require.NoError(t, err)
		assert.Equal(t, len(blk)-1, len(newBlk))
		newBlkIdx, _, err := objects.GetBlockIndex(db, bb, newTbl.BlockIndices[0])
		require.NoError(t, err)
		assert.Equal(t, len(blk)-1, len(newBlkIdx.Rows))
	}
}
