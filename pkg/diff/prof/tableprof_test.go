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
				Columns: []*ColumnProfileDiff{
					{
						Name:    "A",
						Removed: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								Old:       10,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								Old:       20,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								Old:       15,
							},
						},
					},
					{
						Name:    "B",
						Removed: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								Old:       1,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								Old:       2,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								Old:       1,
							},
						},
					},
				},
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
				Columns: []*ColumnProfileDiff{
					{
						Name:        "A",
						NewAddition: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								New:       10,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								New:       20,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								New:       15,
							},
						},
					},
					{
						Name:        "B",
						NewAddition: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								New:       1,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								New:       2,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								New:       1,
							},
						},
					},
				},
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
				Columns: []*ColumnProfileDiff{
					{
						Name:        "A",
						NewAddition: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								New:       10,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								New:       20,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								New:       15,
							},
						},
					},
					{
						Name: "B",
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								Old:       13,
								New:       1,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								Old:       2,
								New:       2,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								Old:       13,
								New:       1,
							},
						},
					},
					{
						Name:    "C",
						Removed: true,
						Stats: []interface{}{
							&Uint16Stat{
								Name:      "Min length",
								ShortName: "minStrLen",
								Old:       12,
							},
							&Uint16Stat{
								Name:      "Max length",
								ShortName: "maxStrLen",
								Old:       22,
							},
							&Uint16Stat{
								Name:      "Avg. length",
								ShortName: "avgStrLen",
								Old:       12,
							},
						},
					},
				},
			},
		},
	} {
		assert.Equal(t, c.tblDiff, DiffTableProfiles(c.newSum, c.oldSum))
	}
}
