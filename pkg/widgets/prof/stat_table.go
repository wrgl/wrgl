// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgetsprof

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/widgets"
)

var (
	cellStyle     = tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlack)
	columnStyle   = tcell.StyleDefault.Background(tcell.ColorBlack).Bold(true)
	pctStyle      = tcell.StyleDefault.Foreground(tcell.ColorLightGray).Background(tcell.ColorBlack)
	statNameStyle = tcell.StyleDefault.Foreground(tcell.ColorAqua).Background(tcell.ColorBlack).Bold(true)
	addedStyle    = cellStyle.Foreground(tcell.ColorGreen)
	removedStyle  = cellStyle.Foreground(tcell.ColorRed)
	movedStyle    = cellStyle.Foreground(tcell.ColorYellow)

	statCells = []StatCells{
		newSingleStatCells("NA count", func(colProf *objects.ColumnProfile) string {
			return fmt.Sprintf("%d", colProf.NACount)
		}),
		newSingleStatCells("Min", func(colProf *objects.ColumnProfile) string {
			if colProf.Min == nil {
				return ""
			}
			return fmt.Sprintf("%f", *colProf.Min)
		}),
		newSingleStatCells("Max", func(colProf *objects.ColumnProfile) string {
			if colProf.Max == nil {
				return ""
			}
			return fmt.Sprintf("%f", *colProf.Max)
		}),
		newSingleStatCells("Mean", func(colProf *objects.ColumnProfile) string {
			if colProf.Mean == nil {
				return ""
			}
			return fmt.Sprintf("%f", *colProf.Mean)
		}),
		newSingleStatCells("Median", func(colProf *objects.ColumnProfile) string {
			if colProf.Median == nil {
				return ""
			}
			return fmt.Sprintf("%f", *colProf.Median)
		}),
		newSingleStatCells("Std. Deviation", func(colProf *objects.ColumnProfile) string {
			if colProf.StdDeviation == nil {
				return ""
			}
			return fmt.Sprintf("%f", *colProf.StdDeviation)
		}),
		newSingleStatCells("Min length", func(colProf *objects.ColumnProfile) string {
			if colProf.MinStrLen == 0 {
				return ""
			}
			return fmt.Sprintf("%d", colProf.MinStrLen)
		}),
		newSingleStatCells("Max length", func(colProf *objects.ColumnProfile) string {
			if colProf.MaxStrLen == 0 {
				return ""
			}
			return fmt.Sprintf("%d", colProf.MaxStrLen)
		}),
		newSingleStatCells("Avg length", func(colProf *objects.ColumnProfile) string {
			if colProf.AvgStrLen == 0 {
				return ""
			}
			return fmt.Sprintf("%d", colProf.AvgStrLen)
		}),
		newTopValuesCells("Top values", func(colProf *objects.ColumnProfile) objects.ValueCounts { return colProf.TopValues }),
		newPercentilesCells("Percentiles", func(colProf *objects.ColumnProfile) []float64 { return colProf.Percentiles }),
	}
)

type StatTable struct {
	*widgets.SelectableTable
	pool        *widgets.CellsPool
	tblProf     *objects.TableProfile
	rowsPerStat []int
	statCells   []StatCells
}

func NewStatTable(tblProf *objects.TableProfile) *StatTable {
	t := &StatTable{
		SelectableTable: widgets.NewSelectableTable(),
		tblProf:         tblProf,
	}
	t.SelectableTable.SetGetCellsFunc(t.getCells).
		SetMinSelection(1, 1).
		Select(1, 1, 0)
	totalRows := t.calculateRowsCount()
	t.VirtualTable.SetFixed(1, 1).
		SetShape(totalRows+1, len(tblProf.Columns)+1)
	t.pool = widgets.NewCellsPool(t.VirtualTable)
	return t
}

func (t *StatTable) calculateRowsCount() int {
	totalRows := 0
	for _, sc := range statCells {
		c := 0
		for _, col := range t.tblProf.Columns {
			if v := sc.NumRows(col); v > c {
				c = v
			}
		}
		if c > 0 {
			t.rowsPerStat = append(t.rowsPerStat, c)
			totalRows += c
			t.statCells = append(t.statCells, sc)
		}
	}
	return totalRows
}

func (t *StatTable) getCells(row, column int) []*widgets.TableCell {
	if row == 0 {
		if column == 0 {
			return nil
		}
		cells, ok := t.pool.Get(row, column, 1)
		if !ok {
			cells[0].SetText(t.tblProf.Columns[column-1].Name).
				SetStyle(columnStyle).
				SetAlign(tview.AlignCenter)
		}
		return cells
	}
	sum := 0
	var sc StatCells
	var statRow int
	for i, rowsCount := range t.rowsPerStat {
		sum += rowsCount
		if sum > row-1 {
			sc = t.statCells[i]
			statRow = row - 1 - sum + rowsCount
			break
		}
	}
	if column == 0 {
		cells, ok := t.pool.Get(row, column, 1)
		if !ok && statRow == 0 {
			cells[0].SetText(sc.Name()).SetStyle(statNameStyle)
		}
		return cells
	}
	cells, _ := t.pool.Get(row, column, sc.NumColumns())
	cp := t.tblProf.Columns[column-1]
	if statRow < sc.NumRows(cp) {
		sc.DecorateCells(statRow, t.tblProf, cp, cells)
	}
	return cells
}
