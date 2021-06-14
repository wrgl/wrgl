package widgets

import (
	"encoding/hex"
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
)

var (
	boldStyle       = tcell.StyleDefault.Background(tcell.ColorBlack).Bold(true)
	boldRedStyle    = boldStyle.Foreground(tcell.ColorRed)
	boldGreenStyle  = boldStyle.Foreground(tcell.ColorGreen)
	boldYellowStyle = boldStyle.Foreground(tcell.ColorYellow)
	yellowStyle     = cellStyle.Foreground(tcell.ColorYellow)
	greenStyle      = cellStyle.Foreground(tcell.ColorGreen)
	redStyle        = cellStyle.Foreground(tcell.ColorRed)
)

const (
	editSet int = iota
	editRemoveCol
	editRemoveRow
)

type editOp struct {
	Type          int
	Row           int
	Layer         int
	Column        int
	OldVal        string
	ColWasRemoved bool
	RowWasRemoved bool
}

type MergeTable struct {
	*SelectableTable

	db          kv.DB
	cd          *objects.ColDiff
	columnCells []*TableCell
	removedCols map[int]struct{}
	removedRows map[int]struct{}
	rowPool     *MergeRowPool

	undoHandler         func()
	redoHandler         func()
	setCellHandler      func(row, column, layer int)
	deleteColumnHandler func(column int)
	deleteRowHandler    func(row int)
}

func NewMergeTable(db kv.DB, fs kv.FileStore, commitNames []string, commitSums [][]byte, cd *objects.ColDiff, merges []*merge.Merge, removedCols map[int]struct{}, removedRows map[int]struct{}) *MergeTable {
	t := &MergeTable{
		db:              db,
		SelectableTable: NewSelectableTable(),
		cd:              cd,
		removedCols:     removedCols,
		removedRows:     removedRows,
	}
	names := make([]string, cd.Layers()+1)
	for i, name := range commitNames {
		names[i] = fmt.Sprintf("%s (%s)", name, hex.EncodeToString(commitSums[i])[:7])
	}
	names[cd.Layers()] = "resolution"
	t.rowPool = NewMergeRowPool(db, cd, names, merges)
	t.SelectableTable.SetGetCellsFunc(t.getMergeCells).
		SetSelectedFunc(t.selectCell).
		SetMinSelection(1, 1).
		Select(1, 1, 0).
		SetShape(1+(t.cd.Layers()+1)*len(merges), cd.Len()+1).
		SetFixed(1, 1)
	numPK := len(cd.OtherPK[0])
	if numPK > 0 {
		t.SelectableTable.SetMinSelection(1, numPK+1).
			Select(1, numPK+1, 0).
			SetFixed(1, numPK+1)
	}
	t.createCells()
	return t
}

func (t *MergeTable) SetUndoHandler(f func()) *MergeTable {
	t.undoHandler = f
	return t
}

func (t *MergeTable) SetRedoHandler(f func()) *MergeTable {
	t.redoHandler = f
	return t
}

func (t *MergeTable) SetSetCellHandler(f func(row, column, layer int)) *MergeTable {
	t.setCellHandler = f
	return t
}

func (t *MergeTable) SetDeleteColumnHandler(f func(column int)) *MergeTable {
	t.deleteColumnHandler = f
	return t
}

func (t *MergeTable) SetDeleteRowHandler(f func(row int)) *MergeTable {
	t.deleteRowHandler = f
	return t
}

func (t *MergeTable) createCells() {
	numCols := t.cd.Len()
	t.columnCells = make([]*TableCell, numCols)
colsLoop:
	for i, name := range t.cd.Names {
		t.columnCells[i] = NewTableCell(name).SetStyle(boldStyle)
		for _, l := range t.cd.Added {
			if _, ok := l[uint32(i)]; ok {
				t.columnCells[i].SetStyle(boldGreenStyle)
				continue colsLoop
			}
		}
		for _, l := range t.cd.Removed {
			if _, ok := l[uint32(i)]; ok {
				t.columnCells[i].SetStyle(boldRedStyle)
				break
			}
		}
	}
}

func (t *MergeTable) mergeRowAtRow(row int) (int, int) {
	nLayers := t.cd.Layers()
	mergeInd := (row - 1) / (nLayers + 1)
	row = row - (nLayers+1)*mergeInd - 1
	return mergeInd, row
}

func (t *MergeTable) getMergeCells(row, column int) []*TableCell {
	if row == 0 {
		if column == 0 {
			return []*TableCell{NewTableCell("")}
		}
		return []*TableCell{t.columnCells[column-1]}
	}
	mergeInd, row := t.mergeRowAtRow(row)
	cell := t.rowPool.GetCell(mergeInd, column, row)
	if row == t.cd.Layers() && column > 0 {
		if _, ok := t.removedCols[column-1]; ok {
			cell = NewTableCell("REMOVED").SetStyle(redStyle)
		} else if _, ok := t.removedRows[mergeInd]; ok {
			cell = NewTableCell("REMOVED").SetStyle(redStyle)
		}
	}
	return []*TableCell{cell}
}

func (t *MergeTable) SetCellFromLayer(row, column, layer int) {
	t.rowPool.SetCellFromLayer(row, column, layer)
}

func (t *MergeTable) SetCell(row, column int, val string) {
	t.rowPool.SetCell(row, column, val)
}

func (t *MergeTable) GetCellText(row, column, subrow int) string {
	return t.rowPool.GetCell(row, column+1, subrow).Text
}

func (t *MergeTable) Select(row, column int) {
	t.SelectableTable.Select((row+1)*(t.cd.Layers()+1), column+1, 0)
}

func (t *MergeTable) selectCell(row, column, subCol int) {
	nLayers := t.cd.Layers()
	rowCount, colCount := t.SelectableTable.GetShape()
	if row <= 0 || row >= rowCount || column <= len(t.cd.OtherPK[0]) || column >= colCount {
		return
	}
	mergeInd, row := t.mergeRowAtRow(row)
	if row == nLayers {

	} else {
		_, different := t.rowPool.IsTextAtCellDifferentFromBase(mergeInd, column, row)
		if different {
			t.setCellHandler(mergeInd, column-1, row)
		}
	}
}

func (t *MergeTable) deleteColumn() {
	_, selectedCol, _ := t.SelectableTable.GetSelection()
	if selectedCol <= len(t.cd.OtherPK[0]) || selectedCol > t.cd.Len() {
		return
	}
	t.deleteColumnHandler(selectedCol - 1)
}

func (t *MergeTable) deleteRow() {
	selectedRow, _, _ := t.SelectableTable.GetSelection()
	rowCount, _ := t.GetShape()
	if selectedRow <= 0 || selectedRow >= rowCount {
		return
	}
	mergeInd, row := t.mergeRowAtRow(selectedRow)
	if row == t.cd.Layers() {
		t.deleteRowHandler(mergeInd)
	}
}

func (t *MergeTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return t.SelectableTable.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) bool {
		key := event.Key()
		switch key {
		case tcell.KeyRune:
			switch event.Rune() {
			case 'u':
				t.undoHandler()
			case 'U':
				t.redoHandler()
			case 'd':
				t.deleteRow()
			case 'D':
				t.deleteColumn()
			default:
				return false
			}
		default:
			return false
		}
		return true
	})
}
