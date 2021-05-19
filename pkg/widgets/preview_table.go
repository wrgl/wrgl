// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"io"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type PreviewTable struct {
	*DataTable
	rowReader        table.RowReader
	headerRow        []*TableCell
	buf              [][]*TableCell
	pkMap            map[uint32]struct{}
	columnsCount     int
	bufStart, bufEnd int
	dec              *objects.StrListDecoder
}

func NewPreviewTable(rowReader table.RowReader, rowCount int, columns []string, primaryKeyIndices []uint32) *PreviewTable {
	headerRow := []*TableCell{}
	for _, text := range columns {
		headerRow = append(headerRow, NewTableCell(text).SetStyle(columnStyle))
	}
	pkMap := map[uint32]struct{}{}
	for _, col := range primaryKeyIndices {
		pkMap[col] = struct{}{}
	}
	t := &PreviewTable{
		DataTable:    NewDataTable(),
		rowReader:    rowReader,
		headerRow:    headerRow,
		columnsCount: len(columns),
		pkMap:        pkMap,
		dec:          objects.NewStrListDecoder(false),
	}
	t.DataTable.SetGetCellsFunc(t.getCells).
		SetShape(rowCount+1, len(columns)).
		SetPrimaryKeyIndices(primaryKeyIndices)
	return t
}

func (t *PreviewTable) SetRowCount(num int) *PreviewTable {
	t.DataTable.SetShape(num+1, t.columnsCount)
	return t
}

func (t *PreviewTable) decodeRow(b []byte) []*TableCell {
	record := t.dec.Decode(b)
	sl := []*TableCell{}
	for _, text := range record {
		sl = append(sl, NewTableCell(text))
	}
	return sl
}

func (t *PreviewTable) readRowAt(row int) []*TableCell {
	_, rowContent, err := t.rowReader.ReadAt(row)
	if err != nil {
		panic(err)
	}
	return t.decodeRow(rowContent)
}

func (t *PreviewTable) readRowsFrom(start, end int) [][]*TableCell {
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

func (t *PreviewTable) getCells(row, column int) []*TableCell {
	if row == 0 {
		return t.headerRow[column : column+1]
	}
	row = row - 1
	// if a distant row is requested, discard all buffer
	if t.bufStart-row >= 200 || row-t.bufEnd >= 200 {
		t.bufStart = row
		t.bufEnd = row + 1
		t.buf = [][]*TableCell{t.readRowAt(t.bufStart)}
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

func (t *PreviewTable) styledCells(row, column int) []*TableCell {
	cell := t.buf[row][column]
	if _, ok := t.pkMap[uint32(column)]; ok {
		cell.SetStyle(primaryKeyStyle)
	} else {
		cell.SetStyle(cellStyle)
	}
	return []*TableCell{cell}
}
