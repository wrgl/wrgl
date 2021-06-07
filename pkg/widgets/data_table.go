// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"strconv"

	"github.com/gdamore/tcell/v2"
)

var (
	columnStyle     = tcell.StyleDefault.Foreground(tcell.ColorAzure).Bold(true)
	rowCountStyle   = tcell.StyleDefault.Foreground(tcell.ColorSlateGray)
	primaryKeyStyle = tcell.StyleDefault.Foreground(tcell.ColorAquaMarine).Background(tcell.ColorBlack)
	cellStyle       = tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlack)
)

// DataTable add following behaviors on top of SelectableTable:
// - Add row number column to the left corner
// - Hoist primary key columns to the left right next to row number column
// - Make row number and primary key columns fixed in place
// - Make column names (first row) fixed in place
// - Apply default styling to the fixed columns and rows
type DataTable struct {
	*SelectableTable

	// This function is called to get the underlying table cell at any position
	getCells func(row, column int) []*TableCell

	// Rearranged indices of columns. Used to hoist primary key columns to the beginning
	columnIndices []uint32

	// Number of primary key columns
	pkCount int

	// Status texts for columns
	columnStatuses []*TableCell
}

func NewDataTable() *DataTable {
	t := &DataTable{
		SelectableTable: NewSelectableTable(),
	}
	t.SelectableTable.SetGetCellsFunc(t.getStyledCells).
		SetMinSelection(1, 0).
		Select(1, 1, 0)
	t.VirtualTable.SetFixed(1, 1)
	return t
}

// SetShape sets total number of rows and columns
func (t *DataTable) SetShape(rowCount, columnCount int) *DataTable {
	t.columnCount = columnCount + 1
	if t.columnStatuses == nil {
		t.rowCount = rowCount + 1
	} else {
		t.rowCount = rowCount + 2
	}
	if t.columnIndices == nil {
		t.SetPrimaryKeyIndices(nil)
	}
	return t
}

// GetShape get number of rows and number of columns
func (t *DataTable) GetShape() (rowCount, columnCount int) {
	rowCount, columnCount = t.SelectableTable.GetShape()
	columnCount--
	if t.columnStatuses == nil {
		rowCount--
	} else {
		rowCount -= 2
	}
	return
}

func (t *DataTable) SetColumnStatuses(cells []*TableCell) *DataTable {
	t.columnStatuses = cells
	t.SelectableTable.SetMinSelection(0, 2).
		Select(1, 2, 0)
	t.VirtualTable.SetFixed(2, t.pkCount+1)
	return t
}

// SetPrimaryKeyIndices records primary key columns and hoist them to the beginning
func (t *DataTable) SetPrimaryKeyIndices(pk []uint32) *DataTable {
	pkm := map[uint32]struct{}{}
	for _, i := range pk {
		pkm[i] = struct{}{}
	}
	ordinaryCols := []uint32{}
	for i := 0; i < t.columnCount; i++ {
		if _, ok := pkm[uint32(i)]; !ok {
			ordinaryCols = append(ordinaryCols, uint32(i))
		}
	}
	t.columnIndices = append(pk, ordinaryCols...)
	t.pkCount = len(pk)
	if t.columnStatuses != nil {
		t.VirtualTable.SetFixed(2, t.pkCount+1)
	} else {
		t.VirtualTable.SetFixed(1, t.pkCount+1)
	}
	return t
}

// SetGetCellsFunc set function to get table cell at a position
func (t *DataTable) SetGetCellsFunc(getCells func(row, column int) []*TableCell) *DataTable {
	t.getCells = getCells
	return t
}

func (t *DataTable) getCellsAt(row, column int) []*TableCell {
	if row < 0 || column < 0 || row >= t.rowCount || column >= t.columnCount {
		return nil
	}
	if t.columnStatuses != nil {
		if row == 0 {
			if column == 0 {
				return []*TableCell{NewTableCell("")}
			}
			statusCell := t.columnStatuses[column-1]
			if statusCell != nil {
				return []*TableCell{statusCell}
			}
			return nil
		}
		row -= 1
	}
	if column == 0 {
		if row == 0 {
			return []*TableCell{NewTableCell("")}
		}
		return []*TableCell{NewTableCell(strconv.Itoa(row - 1))}
	}
	return t.getCells(row, int(t.columnIndices[column-1]))
}

func (t *DataTable) getStyledCells(row, column int) []*TableCell {
	cells := t.getCellsAt(row, column)
	if cells == nil {
		return nil
	}
	if column == 0 {
		cells[0].SetStyle(rowCountStyle)
		return cells
	}
	return cells
}
