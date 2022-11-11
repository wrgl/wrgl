// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package factory

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/pckhoi/meow"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
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

func ingestTable(t *testing.T, db objects.Store, rows [][]string, pk []uint32) []byte {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(rows))
	s, err := sorter.NewSorter()
	require.NoError(t, err)
	sum, err := ingest.IngestTable(db, s, io.NopCloser(bytes.NewReader(buf.Bytes())), slice.IndicesToValues(rows[0], pk), testr.New(t))
	require.NoError(t, err)
	return sum
}

func ingestTableFromRows(t *testing.T, db objects.Store, buf *bytes.Buffer, rows [][]string) ([]byte, *objects.Table) {
	t.Helper()
	hash := meow.New(0)
	enc := objects.NewStrListEncoder(true)
	buf.Reset()
	blk := rows[1:]
	cols := rows[0]
	pk := []uint32{0}

	_, err := objects.WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	var bb []byte
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

	tbl := &objects.Table{
		Columns:      cols,
		PK:           pk,
		RowsCount:    uint32(len(blk)),
		Blocks:       [][]byte{blkSum},
		BlockIndices: [][]byte{blkIdxSum},
	}
	buf.Reset()
	_, err = tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(db, buf.Bytes())
	require.NoError(t, err)
	return sum, tbl
}

func BuildTable(t *testing.T, db objects.Store, rows []string, pk []uint32) []byte {
	t.Helper()
	records, pk := parseRows(rows, pk)
	return ingestTable(t, db, records, pk)
}

func BuildTableN(t *testing.T, db objects.Store, numCols, numRows int, pk []uint32) []byte {
	t.Helper()
	rows := testutils.BuildRawCSV(numCols, numRows)
	return ingestTable(t, db, rows, pk)
}

func SdumpTable(t *testing.T, db objects.Store, sum []byte, indent int) string {
	t.Helper()
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	lines := []string{
		fmt.Sprintf("table %x", sum),
		fmt.Sprintf("    %s", strings.Join(tbl.Columns, ", ")),
	}
	var bb []byte
	var blk [][]string
	for _, sum := range tbl.Blocks {
		lines = append(lines, fmt.Sprintf("  block %x", sum))
		blk, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		for _, row := range blk {
			lines = append(lines, fmt.Sprintf("    %s", strings.Join(row, ", ")))
		}
	}
	if indent > 0 {
		for i, line := range lines {
			lines[i] = strings.Repeat(" ", indent) + line
		}
	}
	return strings.Join(lines, "\n")
}

func SaveTable(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) ([]byte, context.Context) {
	t.Helper()
	buf, ctx := getBuffer(ctx)
	buf.Reset()
	_, err := tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(db, buf.Bytes())
	require.NoError(t, err)
	return sum, ctx
}

func GetTable(t *testing.T, db objects.Store, sum []byte) *objects.Table {
	t.Helper()
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	return tbl
}
