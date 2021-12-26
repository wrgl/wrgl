// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"fmt"
	"math"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/wrgl/wrgl/pkg/objects"
)

type StatCells interface {
	Name() string
	NumRows(colProf *objects.ColumnProfile) int
	NumColumns() int
	DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*TableCell)
}

var (
	statKeyStyle = tcell.StyleDefault.Foreground(tcell.ColorAquaMarine).Background(tcell.ColorBlack)
	statCells    = []StatCells{
		newSingleStatCells("NA count", func(colProf *objects.ColumnProfile) string {
			if colProf.NACount == 0 {
				return ""
			}
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
		newPercentilesCells("Percentiles", func(colProf *objects.ColumnProfile) []float64 { return colProf.Percentiles }),
		newTopValuesCells("Top values", func(colProf *objects.ColumnProfile) objects.ValueCounts { return colProf.TopValues }),
	}
)

type singleStatCells struct {
	name     string
	cellText func(colProf *objects.ColumnProfile) string
}

func (c *singleStatCells) Name() string {
	return c.name
}

func (c *singleStatCells) NumRows(colProf *objects.ColumnProfile) int {
	if c.cellText(colProf) == "" {
		return 0
	}
	return 1
}

func (c *singleStatCells) NumColumns() int {
	return 1
}

func (c *singleStatCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*TableCell) {
	cells[0].SetText(c.cellText(colProf)).SetStyle(cellStyle)
}

func newSingleStatCells(name string, cellText func(colProf *objects.ColumnProfile) string) *singleStatCells {
	return &singleStatCells{
		name:     name,
		cellText: cellText,
	}
}

type topValuesCells struct {
	name   string
	values func(colProf *objects.ColumnProfile) objects.ValueCounts
}

func (c *topValuesCells) Name() string {
	return c.name
}

func (c *topValuesCells) NumRows(colProf *objects.ColumnProfile) int {
	v := c.values(colProf)
	if v == nil {
		return 0
	}
	return v.Len()
}

func (c *topValuesCells) NumColumns() int {
	return 3
}

func (c *topValuesCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*TableCell) {
	values := c.values(colProf)
	v := values[row]
	cells[0].SetText(v.Value).
		SetAlign(tview.AlignRight).
		SetStyle(statKeyStyle)
	cells[1].SetText(fmt.Sprintf("%d", v.Count)).
		SetStyle(cellStyle)
	pct := byte(math.Round(float64(v.Count) / float64(tblProf.RowsCount) * 100))
	cells[2].SetText(fmt.Sprintf("%3d%%", pct)).
		SetStyle(cellStyle)
}

func newTopValuesCells(name string, values func(colProf *objects.ColumnProfile) objects.ValueCounts) *topValuesCells {
	return &topValuesCells{
		name:   name,
		values: values,
	}
}

type percentilesCells struct {
	name   string
	values func(colProf *objects.ColumnProfile) []float64
}

func (c *percentilesCells) Name() string {
	return c.name
}

func (c *percentilesCells) NumRows(colProf *objects.ColumnProfile) int {
	v := c.values(colProf)
	if v == nil {
		return 0
	}
	return len(v)
}

func (c *percentilesCells) NumColumns() int {
	return 2
}

func (c *percentilesCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*TableCell) {
	values := c.values(colProf)
	v := values[row]
	cells[0].SetText(fmt.Sprintf("%d", (row+1)*100/(len(values)+1))).
		SetAlign(tview.AlignRight).
		SetStyle(statKeyStyle)
	cells[1].SetText(fmt.Sprintf("%f", v)).
		SetStyle(cellStyle)
}

func newPercentilesCells(name string, values func(colProf *objects.ColumnProfile) []float64) *percentilesCells {
	return &percentilesCells{
		name:   name,
		values: values,
	}
}

type StatTable struct {
	*SelectableTable
	pool        *CellsPool
	tblProf     *objects.TableProfile
	rowsPerStat []int
	statCells   []StatCells
}

func NewStatTable(tblProf *objects.TableProfile) *StatTable {
	t := &StatTable{
		SelectableTable: NewSelectableTable(),
		tblProf:         tblProf,
	}
	t.SelectableTable.SetGetCellsFunc(t.getCells).
		SetMinSelection(1, 1).
		Select(1, 1, 0)
	totalRows := t.calculateRowsCount()
	t.VirtualTable.SetFixed(1, 1).
		SetShape(totalRows, len(tblProf.Columns)+1)
	t.pool = NewCellsPool(t.VirtualTable)
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

func (t *StatTable) getCells(row, column int) []*TableCell {
	if row == 0 {
		if column == 0 {
			return nil
		}
		cells, ok := t.pool.Get(row, column, 1)
		if !ok {
			cells[0].SetText(t.tblProf.Columns[column-1].Name).SetStyle(columnStyle)
		}
		return cells
	}
	sum := 0
	var sc StatCells
	var statRow int
	for i, rowsCount := range t.rowsPerStat {
		sc = t.statCells[i]
		if sum >= row {
			statRow = sum - row
			break
		}
		sum += rowsCount
	}
	if column == 0 {
		cells, ok := t.pool.Get(row, column, 1)
		if !ok && statRow == 0 {
			cells[0].SetText(sc.Name()).SetStyle(columnStyle)
		}
		return cells
	}
	cells, ok := t.pool.Get(row, column, sc.NumColumns())
	if !ok {
		sc.DecorateCells(statRow, t.tblProf, t.tblProf.Columns[column-1], cells)
	}
	return cells
}
