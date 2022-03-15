// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package widgetsprof

import (
	"fmt"
	"sort"

	"github.com/rivo/tview"
	diffprof "github.com/wrgl/wrgl/pkg/diff/prof"
	"github.com/wrgl/wrgl/pkg/widgets"
)

type StatDiffTable struct {
	*widgets.SelectableTable
	pool        *widgets.CellsPool
	tpd         *diffprof.TableProfileDiff
	diffCells   [][]StatDiffCells
	rowsPerStat []int
	statNames   []string
}

func NewStatDiffTable(tpd *diffprof.TableProfileDiff) (*StatDiffTable, error) {
	t := &StatDiffTable{
		SelectableTable: widgets.NewSelectableTable(),
		tpd:             tpd,
	}
	t.SelectableTable.SetGetCellsFunc(t.getCells).
		SetMinSelection(1, 1).
		Select(1, 1, 0)
	totalRows, err := t.calculateRowsCount()
	if err != nil {
		return nil, err
	}
	t.VirtualTable.SetFixed(1, 1).
		SetShape(totalRows+1, len(tpd.Columns)+1).
		SetSeparator('│')
	t.pool = widgets.NewCellsPool(t.VirtualTable)
	return t, nil
}

func (t *StatDiffTable) calculateRowsCount() (int, error) {
	rows := map[string]int{}
	diffCells := map[string]map[int]StatDiffCells{}
	for i, col := range t.tpd.Columns {
		for _, stat := range col.Stats {
			var sdc StatDiffCells
			switch v := stat.(type) {
			case *diffprof.Uint16Stat:
				sdc = &uint16StatDiffCells{v}
			case *diffprof.Uint32Stat:
				sdc = &uint32StatDiffCells{v}
			case *diffprof.Float64Stat:
				sdc = &float64StatDiffCells{v}
			case *diffprof.TopValuesStat:
				sdc = &topValuesStatDiffCells{v}
			case *diffprof.PercentilesStat:
				sdc = &percentilesStatDiffCells{v}
			default:
				return 0, fmt.Errorf("unanticipated type %T", v)
			}
			if c, ok := rows[sdc.Name()]; !ok || c < sdc.NumRows() {
				rows[sdc.Name()] = sdc.NumRows()
				if !ok {
					t.statNames = append(t.statNames, sdc.Name())
				}
			}
			if _, ok := diffCells[sdc.Name()]; !ok {
				diffCells[sdc.Name()] = map[int]StatDiffCells{}
			}
			diffCells[sdc.Name()][i] = sdc
		}
	}
	totalRows := 0
	sort.Slice(t.statNames, func(i, j int) bool {
		if t.statNames[i] == "Top values" {
			return false
		}
		if t.statNames[j] == "Top values" {
			return true
		}
		if t.statNames[i] == "Percentiles" {
			return false
		}
		if t.statNames[j] == "Percentiles" {
			return true
		}
		if t.statNames[i] == "NA count" {
			return true
		}
		if t.statNames[j] == "NA count" {
			return false
		}
		return i < j
	})
	for _, name := range t.statNames {
		n := len(t.diffCells)
		t.diffCells = append(t.diffCells, make([]StatDiffCells, len(t.tpd.Columns)))
		for i, sdc := range diffCells[name] {
			t.diffCells[n][i] = sdc
		}
		totalRows += rows[name]
		t.rowsPerStat = append(t.rowsPerStat, rows[name])
	}
	return totalRows, nil
}

func (t *StatDiffTable) getCells(row, column int) []*widgets.TableCell {
	if row == 0 {
		if column == 0 {
			return nil
		}
		cells, ok := t.pool.Get(row, column, 1)
		if !ok {
			cells[0].SetText(t.tpd.Columns[column-1].Name).
				SetStyle(columnStyle).
				SetAlign(tview.AlignCenter)
		}
		return cells
	}
	sum := 0
	// var sc StatDiffCells
	var statRow int
	var statInd int
	for i, rowsCount := range t.rowsPerStat {
		sum += rowsCount
		if sum > row-1 {
			// sc = t.diffCells[i][column-1]
			statInd = i
			statRow = row - 1 - sum + rowsCount
			break
		}
	}
	if column == 0 {
		cells, ok := t.pool.Get(row, column, 1)
		if !ok && statRow == 0 {
			cells[0].SetText(t.statNames[statInd]).SetStyle(statNameStyle)
		}
		return cells
	}
	sc := t.diffCells[statInd][column-1]
	if sc != nil {
		cells, ok := t.pool.Get(row, column, sc.NumColumns())
		if !ok && statRow < sc.NumRows() {
			sc.DecorateCells(statRow, cells)
		}
		return cells
	}
	cells, _ := t.pool.Get(row, column, 1)
	return cells
}
