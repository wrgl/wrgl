// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diffprof

import "github.com/wrgl/wrgl/pkg/objects"

type diffobj interface {
	Unchanged() bool
}

type TableProfileDiff struct {
	OldRowsCount uint32               `json:"oldRowsCount"`
	NewRowsCount uint32               `json:"newRowsCount"`
	Columns      []*ColumnProfileDiff `json:"columns"`
}

func (d *TableProfileDiff) Unchanged() bool {
	if d.OldRowsCount != d.NewRowsCount {
		return false
	}
	for _, c := range d.Columns {
		if !c.Unchanged() {
			return false
		}
	}
	return true
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
	if newProf == nil && oldProf == nil {
		return nil
	}
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
			result.Columns = append(result.Columns, cd)
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
			result.Columns = append(result.Columns, cd)
		}
	}
	if result.Unchanged() {
		return nil
	}
	return result
}
