// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestRowListReader(t *testing.T) {
	db := objmock.NewStore()
	rows := testutils.BuildRawCSV(4, 700)
	sorter.SortRows(rows, []uint32{0})
	tbl := ingestRows(t, db, rows)

	r, err := NewRowListReader(db, tbl)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		for j := 0; j < 50; j++ {
			r.Add(uint32(i*255 + j))
		}
	}
	assert.Equal(t, r.Len(), 150)

	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+1], row)
	}

	_, err = r.Seek(50, io.SeekStart)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+256], row)
	}

	_, err = r.Seek(20, io.SeekCurrent)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+286], row)
	}

	_, err = r.Seek(-20, io.SeekEnd)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+541], row)
	}
}
