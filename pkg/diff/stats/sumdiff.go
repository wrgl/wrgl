// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package stats

import "github.com/wrgl/wrgl/pkg/objects"

type ValueDiff struct {
	Name      string
	Old       string
	New       string
	SubValues []*ValueDiff
}

type TableSumDiff struct {
	OldRowsCount   uint32               `json:"oldRowsCount"`
	NewRowsCount   uint32               `json:"newRowsCount"`
	ColumnSumDiffs []*ColumnSummaryDiff `json:"columnSummaryDiffs"`
}

func columnSumMap(tblSum *objects.TableSummary) map[string]*objects.ColumnSummary {
	m := map[string]*objects.ColumnSummary{}
	if tblSum == nil {
		return m
	}
	for _, col := range tblSum.Columns {
		m[col.Name] = col
	}
	return m
}

func DiffTableSummaries(newSum, oldSum *objects.TableSummary) *TableSumDiff {
	result := &TableSumDiff{}
	oldColsM := columnSumMap(oldSum)
	newColsM := columnSumMap(newSum)
	if newSum != nil {
		result.NewRowsCount = newSum.RowsCount
		for _, col := range newSum.Columns {
			cd := &ColumnSummaryDiff{
				Name: col.Name,
			}
			oldCol, ok := oldColsM[col.Name]
			if !ok {
				cd.NewAddition = true
			}
			cd.CollectStats(newSum, oldSum, col, oldCol)
			result.ColumnSumDiffs = append(result.ColumnSumDiffs, cd)
		}
	}
	if oldSum != nil {
		result.OldRowsCount = oldSum.RowsCount
		for _, col := range oldSum.Columns {
			if _, ok := newColsM[col.Name]; ok {
				continue
			}
			cd := &ColumnSummaryDiff{
				Name:    col.Name,
				Removed: true,
			}
			cd.CollectStats(newSum, oldSum, nil, col)
			result.ColumnSumDiffs = append(result.ColumnSumDiffs, cd)
		}
	}
	return result
}
