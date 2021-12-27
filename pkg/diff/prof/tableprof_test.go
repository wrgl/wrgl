// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"encoding/json"
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
				ColumnDiffs: []*ColumnProfileDiff{
					{
						Name:        "A",
						NewAddition: false,
						Removed:     true,
						Stats: []json.Marshaler{
							&uint32Stat{
								Name:      "NA count",
								ShortName: "naCount",
								Old:       0,
								New:       0,
							},
							&float64Stat{
								Name:      "Min",
								ShortName: "min",
							},
							&float64Stat{
								Name:      "Max",
								ShortName: "max",
							},
							&float64Stat{
								Name:      "Mean",
								ShortName: "mean",
							},
							&float64Stat{
								Name:      "Median",
								ShortName: "median",
							},
							&float64Stat{
								Name:      "Standard deviation",
								ShortName: "stdDeviation",
							},
							&uint16Stat{
								Name:      "Min string length",
								ShortName: "minStrLen",
								Old:       10,
								New:       0,
							},
							&uint16Stat{
								Name:      "Max string length",
								ShortName: "maxStrLen",
								Old:       20,
								New:       0,
							},
							&uint16Stat{
								Name:      "Avg string length",
								ShortName: "avgStrLen",
								Old:       15,
								New:       0,
							},
						},
					},
					{
						Name:        "B",
						NewAddition: false,
						Removed:     true,
						Stats: []json.Marshaler{
							&uint32Stat{
								Name:      "NA count",
								ShortName: "naCount",
								Old:       0,
								New:       0,
							},
							&float64Stat{
								Name:      "Min",
								ShortName: "min",
							},
							&float64Stat{
								Name:      "Max",
								ShortName: "max",
							},
							&float64Stat{
								Name:      "Mean",
								ShortName: "mean",
							},
							&float64Stat{
								Name:      "Median",
								ShortName: "median",
							},
							&float64Stat{
								Name:      "Standard deviation",
								ShortName: "stdDeviation",
							},
							&uint16Stat{
								Name:      "Min string length",
								ShortName: "minStrLen",
								Old:       1,
								New:       0,
							},
							&uint16Stat{
								Name:      "Max string length",
								ShortName: "maxStrLen",
								Old:       2,
								New:       0,
							},
							&uint16Stat{
								Name:      "Avg string length",
								ShortName: "avgStrLen",
								Old:       1,
								New:       0,
							},
						},
					},
				},
			},
		},
		// {
		// 	&objects.TableProfile{
		// 		RowsCount: 200,
		// 		Columns: []*objects.ColumnProfile{
		// 			{
		// 				Name:      "A",
		// 				MinStrLen: 10,
		// 				MaxStrLen: 20,
		// 				AvgStrLen: 15,
		// 			},
		// 			{
		// 				Name:      "B",
		// 				MinStrLen: 1,
		// 				MaxStrLen: 2,
		// 				AvgStrLen: 1,
		// 			},
		// 		},
		// 	},
		// 	nil,
		// 	&TableProfileDiff{
		// 		NewRowsCount: 200,
		// 		ColumnDiffs:  []*ColumnProfileDiff{},
		// 	},
		// },
		// {
		// 	&objects.TableProfile{
		// 		RowsCount: 200,
		// 		Columns: []*objects.ColumnProfile{
		// 			{
		// 				Name:      "A",
		// 				MinStrLen: 10,
		// 				MaxStrLen: 20,
		// 				AvgStrLen: 15,
		// 			},
		// 			{
		// 				Name:      "B",
		// 				MinStrLen: 1,
		// 				MaxStrLen: 2,
		// 				AvgStrLen: 1,
		// 			},
		// 		},
		// 	},
		// 	&objects.TableProfile{
		// 		RowsCount: 300,
		// 		Columns: []*objects.ColumnProfile{
		// 			{
		// 				Name:      "B",
		// 				MinStrLen: 13,
		// 				MaxStrLen: 2,
		// 				AvgStrLen: 13,
		// 			},
		// 			{
		// 				Name:      "C",
		// 				MinStrLen: 12,
		// 				MaxStrLen: 22,
		// 				AvgStrLen: 12,
		// 			},
		// 		},
		// 	},
		// 	&TableProfileDiff{
		// 		NewRowsCount: 200,
		// 		OldRowsCount: 300,
		// 		ColumnDiffs:  []*ColumnProfileDiff{},
		// 	},
		// },
	} {
		assert.Equal(t, c.tblDiff, DiffTableProfiles(c.newSum, c.oldSum))
	}
}
