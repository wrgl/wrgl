// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import "github.com/wrgl/wrgl/pkg/objects"

type ValueDiff struct {
	Name      string
	Old       string
	New       string
	SubValues []*ValueDiff
}

type TableProfileDiff struct {
	OldRowsCount uint32               `json:"oldRowsCount"`
	NewRowsCount uint32               `json:"newRowsCount"`
	ColumnDiffs  []*ColumnProfileDiff `json:"columnDiffs"`
}

func columnProfMap(tblProf *objects.TableProfile) map[string]*objects.ColumnProfile {
	m := map[string]*objects.ColumnProfile{}
	if tblProf == nil {
		return m
	}
	for _, col := range tblProf.Columns {
		m[col.Name] = col
	}
	return m
}

func DiffTableProfiles(newProf, oldProf *objects.TableProfile) *TableProfileDiff {
	result := &TableProfileDiff{}
	oldColsM := columnProfMap(oldProf)
	newColsM := columnProfMap(newProf)
	if newProf != nil {
		result.NewRowsCount = newProf.RowsCount
		for _, col := range newProf.Columns {
			cd := &ColumnProfileDiff{
				Name: col.Name,
			}
			oldCol, ok := oldColsM[col.Name]
			if !ok {
				cd.NewAddition = true
			}
			cd.CollectStats(newProf, oldProf, col, oldCol)
			result.ColumnDiffs = append(result.ColumnDiffs, cd)
		}
	}
	if oldProf != nil {
		result.OldRowsCount = oldProf.RowsCount
		for _, col := range oldProf.Columns {
			if _, ok := newColsM[col.Name]; ok {
				continue
			}
			cd := &ColumnProfileDiff{
				Name:    col.Name,
				Removed: true,
			}
			cd.CollectStats(newProf, oldProf, nil, col)
			result.ColumnDiffs = append(result.ColumnDiffs, cd)
		}
	}
	return result
}
