package widgets

import (
	"io"

	"github.com/wrgl/core/pkg/diff"
)

type DiffTable struct {
	*DataTable
	reader           *diff.RowChangeReader
	headerRow        []*TableCell
	buf              [][][]*TableCell
	bufStart, bufEnd int
}

func NewDiffTable(reader *diff.RowChangeReader) *DiffTable {
	t := &DiffTable{
		DataTable: NewDataTable(),
		reader:    reader,
	}
	headerRow := []*TableCell{}
	for _, col := range reader.Columns {
		headerRow = append(headerRow, NewTableCell(col.Name))
	}
	t.DataTable.SetGetCellsFunc(t.getCells).
		SetPrimaryKeyIndices(t.reader.PKIndices)
	t.UpdateRowCount()
	return t
}

func (t *DiffTable) UpdateRowCount() {
	t.DataTable.SetShape(t.reader.NumRows()+1, len(t.reader.Columns))
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
		return t.buf[0][column]
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
	return t.buf[row-t.bufStart][column]
}
