// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ingest

import (
	"os"
	"testing"

	"github.com/go-logr/logr/testr"
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
	require.NoError(t, f.Close())
	defer os.Remove(f.Name())
	logger := testr.New(t)

	for _, pk := range [][]string{rows[0][:1], nil} {
		db := objmock.NewStore()
		s, err := sorter.NewSorter()
		require.NoError(t, err)
		f, err = os.Open(f.Name())
		require.NoError(t, err)
		sum, err := IngestTable(db, s, f, pk, logger)
		require.NoError(t, err)
		tbl, err := objects.GetTable(db, sum)
		require.NoError(t, err)
		tblIdx, err := objects.GetTableIndex(db, sum)
		require.NoError(t, err)
		for _, sl := range tblIdx {
			assert.True(t, len(sl) > 0)
		}
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

		require.NoError(t, IndexTable(db, sum, tbl, testr.New(t)))
		tblIdx2, err := objects.GetTableIndex(db, sum)
		require.NoError(t, err)
		assert.Equal(t, tblIdx, tblIdx2)
		for i, sum := range tbl.BlockIndices {
			idx, bb, err = objects.GetBlockIndex(db, bb, sum)
			require.NoError(t, err)
			assert.Equal(t, blkIndices[i], idx)
		}
	}
}
