// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/sorter"
	"github.com/wrgl/core/pkg/testutils"
)

func ingestRows(t *testing.T, db objects.Store, rows [][]string) *objects.Table {
	t.Helper()
	f, err := ioutil.TempFile("", "*.csv")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	sum, err := ingest.IngestTable(db, f, f.Name(), rows[0][:1], 0, 1, io.Discard)
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	return tbl
}

func TestRowReader(t *testing.T) {
	db := objmock.NewStore()
	rows := testutils.BuildRawCSV(4, 700)
	sorter.SortBlock(rows, []uint32{0})
	tbl := ingestRows(t, db, rows)

	r, err := NewRowReader(db, tbl)
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
