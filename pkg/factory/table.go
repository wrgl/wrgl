// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package factory

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
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

func BuildTable(t *testing.T, db objects.Store, rows []string, pk []uint32) []byte {
	t.Helper()
	records, pk := parseRows(rows, pk)
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(records))
	sum, err := ingest.IngestTable(db, io.NopCloser(bytes.NewReader(buf.Bytes())), slice.IndicesToValues(records[0], pk))
	require.NoError(t, err)
	return sum
}

func SdumpTable(t *testing.T, db objects.Store, sum []byte, indent int) string {
	t.Helper()
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	lines := []string{
		fmt.Sprintf("table %x", sum),
		fmt.Sprintf("    %s", strings.Join(tbl.Columns, ", ")),
	}
	for _, sum := range tbl.Blocks {
		lines = append(lines, fmt.Sprintf("  block %x", sum))
		blk, err := objects.GetBlock(db, sum)
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
