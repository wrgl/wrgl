package factory

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func GetRows(t *testing.T, db objects.Store, tbl *objects.Table) [][]string {
	var rows [][]string
	var bb []byte
	var err error
	var blk [][]string
	for _, sum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		rows = append(rows, blk...)
	}
	return rows
}

func IndexRows(pk []uint32, rows [][]string) map[string][]string {
	m := map[string][]string{}
	for _, row := range rows {
		m[strings.Join(slice.IndicesToValues(row, pk), "-")] = row
	}
	return m
}

func AssertDuplicatedRowsRemoved(t *testing.T, db objects.Store, newTbl, oldTbl *objects.Table) {
	t.Helper()
	assert.Equal(t, newTbl.Columns, oldTbl.Columns)
	assert.Equal(t, newTbl.PK, oldTbl.PK)
	assert.Less(t, newTbl.RowsCount, oldTbl.RowsCount)
	newRows := GetRows(t, db, newTbl)
	oldRows := GetRows(t, db, oldTbl)
	assert.Less(t, len(newRows), len(oldRows))
	assert.Equal(t, IndexRows(newTbl.PK, newRows), IndexRows(oldTbl.PK, oldRows))
}

func CreateRows(t *testing.T, ncols, nrows, ndup int) [][]string {
	t.Helper()
	cols := strings.Split(testutils.LowerAlphaBytes[:ncols], "")
	rows := make([][]string, 0, nrows+1)
	rows = append(rows, cols)
	for i := 0; i < nrows; i++ {
		rows = append(rows, append(
			[]string{strconv.Itoa(i + 1)},
			strings.Split(testutils.BrokenRandomAlphaNumericString(ncols-1), "")...,
		))
	}
	for j := 0; j < ndup; j++ {
		copy(rows[nrows-ndup+j+1], rows[nrows-ndup+j-1+1])
	}
	return rows
}
