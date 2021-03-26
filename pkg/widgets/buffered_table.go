package widgets

import (
	"io"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/table"
)

type BufferedTable struct {
	*DataTable
	rowReader        table.RowReader
	headerRow        []*TableCell
	buf              [][]*TableCell
	columnsCount     int
	bufStart, bufEnd int
}

func NewBufferedTable(rowReader table.RowReader, rowCount int, columns []string, primaryKeyIndices []int) *BufferedTable {
	headerRow := []*TableCell{}
	for _, text := range columns {
		headerRow = append(headerRow, NewTableCell(text))
	}
	t := &BufferedTable{
		DataTable:    NewDataTable(),
		rowReader:    rowReader,
		headerRow:    headerRow,
		columnsCount: len(columns),
	}
	t.DataTable.SetGetCellsFunc(t.getCells).
		SetShape(rowCount, len(columns)).
		SetPrimaryKeyIndices(primaryKeyIndices)
	return t
}

func (t *BufferedTable) SetRowCount(num int) *BufferedTable {
	t.DataTable.SetShape(num, t.columnCount)
	return t
}

func (t *BufferedTable) decodeRow(b []byte) []*TableCell {
	record, err := encoding.DecodeStrings(b)
	if err != nil {
		panic(err)
	}
	sl := []*TableCell{}
	for _, text := range record {
		sl = append(sl, NewTableCell(text))
	}
	return sl
}

func (t *BufferedTable) readRowAt(row int) []*TableCell {
	_, rowContent, err := t.rowReader.ReadAt(row)
	if err != nil {
		panic(err)
	}
	return t.decodeRow(rowContent)
}

func (t *BufferedTable) readRowsFrom(start, end int) [][]*TableCell {
	rows := [][]*TableCell{}
	_, err := t.rowReader.Seek(start, io.SeekStart)
	if err != nil {
		panic(err)
	}
	off := start
	for off < end {
		_, rowContent, err := t.rowReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		rows = append(rows, t.decodeRow(rowContent))
		off++
	}
	return rows
}

func (t *BufferedTable) getCells(row, column int) []*TableCell {
	if row == 0 {
		return t.headerRow[column : column+1]
	}
	row = row - 1
	// if a distant row is requested, discard all buffer
	if t.bufStart-row >= 200 || row-t.bufEnd >= 200 {
		t.bufStart = row
		t.bufEnd = row + 1
		t.buf = [][]*TableCell{t.readRowAt(t.bufStart)}
		return t.buf[0][column : column+1]
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
	return t.buf[row-t.bufStart][column : column+1]
}
