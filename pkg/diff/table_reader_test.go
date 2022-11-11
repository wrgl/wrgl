// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diff

import (
	"encoding/csv"
	"io"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func ingestRows(t *testing.T, db objects.Store, rows [][]string) *objects.Table {
	t.Helper()
	f, err := testutils.TempFile("", "*.csv")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	s, err := sorter.NewSorter()
	require.NoError(t, err)
	logger := testr.New(t)
	sum, err := ingest.IngestTable(db, s, f, rows[0][:1], logger)
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	return tbl
}

func TestRowReader(t *testing.T) {
	db := objmock.NewStore()
	rows := testutils.BuildRawCSV(4, 700)
	sorter.SortRows(rows, []uint32{0})
	tbl := ingestRows(t, db, rows)

	r, err := NewTableReader(db, tbl)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+1], row)
	}

	_, err = r.Seek(250, io.SeekStart)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+251], row)
	}

	_, err = r.Seek(20, io.SeekCurrent)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+281], row)
	}

	_, err = r.Seek(-20, io.SeekEnd)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, rows[i+681], row)
	}
}
