// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"fmt"
	"io"

	"github.com/gdamore/tcell/v2"
	"github.com/wrgl/wrgl/pkg/diff"
)

var (
	addedStyle   = cellStyle.Foreground(tcell.ColorGreen)
	removedStyle = cellStyle.Foreground(tcell.ColorRed)
	movedStyle   = cellStyle.Foreground(tcell.ColorYellow)
)

type DiffTable struct {
	*DataTable
	reader           *diff.RowChangeReader
	headerRow        []*TableCell
	buf              [][][]*TableCell
	statusExist      bool
	bufStart, bufEnd int
}

func NewDiffTable(reader *diff.RowChangeReader) *DiffTable {
	t := &DiffTable{
		DataTable: NewDataTable(),
		reader:    reader,
	}
	headerRow := []*TableCell{}
	colStatuses := []*TableCell{}
	for i, name := range reader.ColDiff.Names {
		headerRow = append(headerRow, NewTableCell(name).SetStyle(columnStyle))
		colStatuses = append(colStatuses, nil)
		if _, ok := reader.ColDiff.Added[0][uint32(i)]; ok {
			colStatuses[i] = NewTableCell("Added").SetStyle(addedStyle)
			t.statusExist = true
		} else if _, ok := reader.ColDiff.Removed[0][uint32(i)]; ok {
			colStatuses[i] = NewTableCell("Removed").SetStyle(removedStyle)
			t.statusExist = true
		} else if m, ok := reader.ColDiff.Moved[0][uint32(i)]; ok && m[0] != -1 {
			colStatuses[i] = NewTableCell(fmt.Sprintf("Moved, used to be before %q", reader.ColDiff.Names[m[0]])).SetStyle(movedStyle)
			t.statusExist = true
		} else if ok && m[1] != -1 {
			colStatuses[i] = NewTableCell(fmt.Sprintf("Moved, used to be after %q", reader.ColDiff.Names[m[1]])).SetStyle(movedStyle)
			t.statusExist = true
		}
	}
	t.DataTable.SetGetCellsFunc(t.getCells)
	if t.statusExist {
		t.DataTable.SetColumnStatuses(colStatuses)
	}
	t.DataTable.SetShape(t.reader.NumRows(), t.reader.ColDiff.Len()).
		SetPrimaryKeyIndices(t.reader.ColDiff.PKIndices())

	t.headerRow = headerRow
	return t
}

func (t *DiffTable) UpdateRowCount() {
	t.DataTable.SetShape(t.reader.NumRows(), t.reader.ColDiff.Len())
}

func (t *DiffTable) rowToCells(row [][]string) [][]*TableCell {
	result := [][]*TableCell{}
	for i, cells := range row {
		result = append(result, []*TableCell{})
		for _, cell := range cells {
			result[i] = append(result[i], NewTableCell(cell))
		}
	}
	return result
}

func (t *DiffTable) readRowAt(offset int) [][]*TableCell {
	row, err := t.reader.ReadAt(offset)
	if err != nil {
		panic(err)
	}
	return t.rowToCells(row)
}

func (t *DiffTable) readRowsFrom(start, end int) [][][]*TableCell {
	rows := [][][]*TableCell{}
	_, err := t.reader.Seek(start, io.SeekStart)
	if err != nil {
		panic(err)
	}
	off := start
	for off < end {
		row, err := t.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		rows = append(rows, t.rowToCells(row))
		off++
	}
	return rows
}

func (t *DiffTable) getCells(row, column int) []*TableCell {
	if row == 0 {
		return t.headerRow[column : column+1]
	}
	row = row - 1

	// if a distant row is requested, discard all buffer
	if t.bufStart-row >= 200 || row-t.bufEnd >= 200 {
		t.bufStart = row
		t.bufEnd = row + 1
		t.buf = [][][]*TableCell{t.readRowAt(t.bufStart)}
		return t.styledCells(0, column)
	}

	if row < t.bufStart {
		rows := t.readRowsFrom(row, t.bufStart)
		t.bufStart = row
		t.buf = append(rows, t.buf...)
	}
	if row >= t.bufEnd {
		rows := t.readRowsFrom(t.bufEnd, row+1)
		t.bufEnd = row + 1
		t.buf = append(t.buf, rows...)
	}
	return t.styledCells(row-t.bufStart, column)
}

func (t *DiffTable) styledCells(row, column int) []*TableCell {
	cells := t.buf[row][column]
	if column < t.pkCount {
		cells[0].SetStyle(primaryKeyStyle)
	} else if len(cells) == 2 {
		cells[0].SetStyle(addedStyle).SetExpansion(1)
		cells[1].SetStyle(removedStyle).SetExpansion(1)
	} else if _, ok := t.reader.ColDiff.Added[0][uint32(column)]; ok {
		cells[0].SetStyle(addedStyle)
	} else if _, ok := t.reader.ColDiff.Removed[0][uint32(column)]; ok {
		cells[0].SetStyle(removedStyle)
	} else {
		cells[0].SetStyle(cellStyle)
	}
	return cells
}
