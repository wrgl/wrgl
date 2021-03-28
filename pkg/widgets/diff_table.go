package widgets

import (
	"fmt"
	"io"

	"github.com/gdamore/tcell/v2"
	"github.com/wrgl/core/pkg/diff"
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
	for i, col := range reader.Columns {
		headerRow = append(headerRow, NewTableCell(col.Name).SetStyle(columnStyle))
		colStatuses = append(colStatuses, nil)
		if col.Added {
			colStatuses[i] = NewTableCell("Added").SetStyle(addedStyle)
			t.statusExist = true
		} else if col.Removed {
			colStatuses[i] = NewTableCell("Removed").SetStyle(removedStyle)
			t.statusExist = true
		} else if col.MovedFrom != -1 {
			colStatuses[i] = NewTableCell(fmt.Sprintf("Moved from position %d", col.MovedFrom)).SetStyle(movedStyle)
			t.statusExist = true
		}
	}
	t.DataTable.SetGetCellsFunc(t.getCells)
	if !t.statusExist {
		t.DataTable.SetShape(t.reader.NumRows()+1, len(t.reader.Columns))
	} else {
		t.DataTable.SetShape(t.reader.NumRows()+2, len(t.reader.Columns)).
			SetColumnStatuses(colStatuses)
	}
	t.DataTable.SetPrimaryKeyIndices(t.reader.PKIndices)
	t.headerRow = headerRow
	return t
}

func (t *DiffTable) UpdateRowCount() {
	if !t.statusExist {
		t.DataTable.SetShape(t.reader.NumRows()+1, len(t.reader.Columns))
	} else {
		t.DataTable.SetShape(t.reader.NumRows()+2, len(t.reader.Columns))
	}
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
	} else if t.reader.Columns[column].Added {
		cells[0].SetStyle(addedStyle)
	} else if t.reader.Columns[column].Removed {
		cells[0].SetStyle(removedStyle)
	} else if t.reader.Columns[column].MovedFrom != -1 {
		cells[0].SetStyle(movedStyle)
	} else {
		cells[0].SetStyle(cellStyle)
	}
	return cells
}
