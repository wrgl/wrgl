// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestDiffTableSummaries(t *testing.T) {
	for _, c := range []struct {
		newSum  *objects.TableSummary
		oldSum  *objects.TableSummary
		tblDiff *TableSumDiff
	}{
		{nil, nil, &TableSumDiff{}},
		{
			nil,
			&objects.TableSummary{
				RowsCount: 200,
				Columns: []*objects.ColumnSummary{
					{
						Name:      "A",
						MinStrLen: 10,
						MaxStrLen: 20,
						AvgStrLen: 15,
					},
					{
						Name:      "B",
						MinStrLen: 1,
						MaxStrLen: 2,
						AvgStrLen: 1,
					},
				},
			},
			&TableSumDiff{
				OldRowsCount:   200,
				ColumnSumDiffs: []*ColumnSummaryDiff{},
			},
		},
		{
			&objects.TableSummary{
				RowsCount: 200,
				Columns: []*objects.ColumnSummary{
					{
						Name:      "A",
						MinStrLen: 10,
						MaxStrLen: 20,
						AvgStrLen: 15,
					},
					{
						Name:      "B",
						MinStrLen: 1,
						MaxStrLen: 2,
						AvgStrLen: 1,
					},
				},
			},
			nil,
			&TableSumDiff{
				NewRowsCount:   200,
				ColumnSumDiffs: []*ColumnSummaryDiff{},
			},
		},
		{
			&objects.TableSummary{
				RowsCount: 200,
				Columns: []*objects.ColumnSummary{
					{
						Name:      "A",
						MinStrLen: 10,
						MaxStrLen: 20,
						AvgStrLen: 15,
					},
					{
						Name:      "B",
						MinStrLen: 1,
						MaxStrLen: 2,
						AvgStrLen: 1,
					},
				},
			},
			&objects.TableSummary{
				RowsCount: 300,
				Columns: []*objects.ColumnSummary{
					{
						Name:      "B",
						MinStrLen: 13,
						MaxStrLen: 2,
						AvgStrLen: 13,
					},
					{
						Name:      "C",
						MinStrLen: 12,
						MaxStrLen: 22,
						AvgStrLen: 12,
					},
				},
			},
			&TableSumDiff{
				NewRowsCount:   200,
				OldRowsCount:   300,
				ColumnSumDiffs: []*ColumnSummaryDiff{},
			},
		},
	} {
		assert.Equal(t, c.tblDiff, DiffTableSummaries(c.newSum, c.oldSum))
	}
}
