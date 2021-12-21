package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func floatPtr(f float64) *float64 {
	return &f
}

func TestWriteTableSummary(t *testing.T) {
	tbl := TableSummary{
		RowsCount: 200,
		Columns: []*ColumnSummary{
			{
				Name:         "a",
				NullCount:    0,
				IsNumber:     true,
				Min:          floatPtr(0),
				Max:          floatPtr(200),
				Mean:         floatPtr(3.123),
				Median:       floatPtr(5),
				Mode:         floatPtr(6),
				StdDeviation: floatPtr(3.4),
				Percentiles: []float64{
					3, 7, 10, 14.69, 17, 21.69, 24, 28.69, 31, 34, 38, 41, 45, 48, 52.69, 55, 59.69, 62, 66.69,
				},
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
