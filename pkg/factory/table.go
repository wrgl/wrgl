package factory

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
)

func parseRows(rows []string, pk []uint32) ([][]string, []uint32) {
	records := [][]string{}
	if rows == nil {
		for i := 0; i < 4; i++ {
			row := []string{}
			for j := 0; j < 3; j++ {
				row = append(row, testutils.BrokenRandomLowerAlphaString(3))
			}
			records = append(records, row)
		}
	} else {
		for _, row := range rows {
			records = append(records, strings.Split(row, ","))
		}
	}
	if pk == nil {
		pk = []uint32{0}
	}
	return records, pk
}

func fillTable(t *testing.T, db kv.DB, ts table.Store, records [][]string, pk []uint32) []byte {
	t.Helper()
	rh := ingest.NewRowHasher(pk, 0)
	for i, rec := range records[1:] {
		pkHash, rowHash, rowContent, err := rh.Sum(rec)
		require.NoError(t, err)
		err = ts.InsertRow(i, pkHash, rowHash, rowContent)
		require.NoError(t, err)
	}
	sum, err := ts.Save()
	require.NoError(t, err)
	return sum
}

func BuildSmallTable(t *testing.T, db kv.DB, rows []string, pk []uint32) ([]byte, table.Store) {
	t.Helper()
	records, pk := parseRows(rows, pk)
	ts := table.NewSmallStore(db, records[0], pk, 0)
	sum := fillTable(t, db, ts, records, pk)
	return sum, ts
}

func BuildBigTable(t *testing.T, db kv.Store, fs kv.FileStore, rows []string, pk []uint32) ([]byte, table.Store) {
	t.Helper()
	records, pk := parseRows(rows, pk)
	ts, err := table.NewBigStore(db, fs, records[0], pk, 0)
	require.NoError(t, err)
	sum := fillTable(t, db, ts, records, pk)
	return sum, ts
}
