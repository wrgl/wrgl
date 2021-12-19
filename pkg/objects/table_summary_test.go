package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestWriteTableSummary(t *testing.T) {
	var zero float64
	var twohun float64 = 200
	tbl := TableSummary{
		RowsCount: 200,
		Columns: []*ColumnSummary{
			{
				Name:      "a",
				NullCount: 0,
				IsNumber:  true,
				Min:       &zero,
				Max:       &twohun,
				AvgStrLen: 2,
			},
			{
				Name:      "bc",
				NullCount: 10,
				AvgStrLen: 5,
				TopValues: ValueCounts{
					{testutils.BrokenRandomLowerAlphaString(5), 50},
					{testutils.BrokenRandomLowerAlphaString(5), 30},
					{testutils.BrokenRandomLowerAlphaString(5), 20},
					{testutils.BrokenRandomLowerAlphaString(5), 10},
				},
			},
			{
				Name:      "def",
				NullCount: 20,
				AvgStrLen: 10,
				TopValues: ValueCounts{
					{testutils.BrokenRandomLowerAlphaString(10), 50},
					{testutils.BrokenRandomLowerAlphaString(10), 30},
					{testutils.BrokenRandomLowerAlphaString(10), 20},
					{testutils.BrokenRandomLowerAlphaString(10), 10},
				},
			},
		},
	}

	w := bytes.NewBuffer(nil)
	n, err := tbl.WriteTo(w)
	require.NoError(t, err)

	tbl2 := &TableSummary{}
	m, err := tbl2.ReadFrom(bytes.NewReader(w.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, m)
	assert.Equal(t, tbl, *tbl2)
}
