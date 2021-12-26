// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestDiffTableSummaries(t *testing.T) {
	for _, c := range []struct {
		newSum  *objects.TableProfile
		oldSum  *objects.TableProfile
		tblDiff *TableProfileDiff
	}{
		{nil, nil, &TableProfileDiff{}},
		{
			nil,
			&objects.TableProfile{
				RowsCount: 200,
				Columns: []*objects.ColumnProfile{
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
			&TableProfileDiff{
				OldRowsCount: 200,
				ColumnDiffs:  []*ColumnProfileDiff{},
			},
		},
		{
			&objects.TableProfile{
				RowsCount: 200,
				Columns: []*objects.ColumnProfile{
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
			&TableProfileDiff{
				NewRowsCount: 200,
				ColumnDiffs:  []*ColumnProfileDiff{},
			},
		},
		{
			&objects.TableProfile{
				RowsCount: 200,
				Columns: []*objects.ColumnProfile{
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
			&objects.TableProfile{
				RowsCount: 300,
				Columns: []*objects.ColumnProfile{
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
			&TableProfileDiff{
				NewRowsCount: 200,
				OldRowsCount: 300,
				ColumnDiffs:  []*ColumnProfileDiff{},
			},
		},
	} {
		assert.Equal(t, c.tblDiff, DiffTableProfiles(c.newSum, c.oldSum))
	}
}
