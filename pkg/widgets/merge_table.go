package widgets

import (
	"container/list"
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
	dec         *objects.StrListDecoder
	columnCells []*TableCell
	PK          []byte
	RemovedCols map[int]struct{}
	RemovedRows map[int]struct{}
	undoStack   *list.List
	redoStack   *list.List
	rowPool     *MergeRowPool
}

func NewMergeTable(db kv.DB, fs kv.FileStore, commitNames []string, commitSums [][]byte, cd *objects.ColDiff, merges []*merge.Merge) *MergeTable {
	t := &MergeTable{
		db:              db,
		SelectableTable: NewSelectableTable(),
		dec:             objects.NewStrListDecoder(false),
		cd:              cd,
		RemovedCols:     map[int]struct{}{},
		RemovedRows:     map[int]struct{}{},
		undoStack:       list.New(),
		redoStack:       list.New(),
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
		if _, ok := t.RemovedCols[column-1]; ok {
			cell = NewTableCell("REMOVED").SetStyle(redStyle)
		} else if _, ok := t.RemovedRows[mergeInd]; ok {
			cell = NewTableCell("REMOVED").SetStyle(redStyle)
		}
	}
	return []*TableCell{cell}
}

func (t *MergeTable) execOp(op *editOp) {
	switch op.Type {
	case editRemoveCol:
		t.RemovedCols[op.Column] = struct{}{}
	case editRemoveRow:
		t.RemovedRows[op.Row] = struct{}{}
	case editSet:
		delete(t.RemovedCols, op.Column)
		delete(t.RemovedRows, op.Row)
		t.rowPool.SetCellFromLayer(op.Row, op.Column, op.Layer)
	}
	t.undoStack.PushFront(op)
}

func (t *MergeTable) undo() {
	e := t.undoStack.Front()
	if e == nil {
		return
	}
	t.undoStack.Remove(e)
	op := e.Value.(*editOp)
	switch op.Type {
	case editRemoveCol:
		delete(t.RemovedCols, op.Column)
	case editRemoveRow:
		delete(t.RemovedRows, op.Row)
	case editSet:
		t.rowPool.SetCell(op.Row, op.Column, op.OldVal)
		if op.ColWasRemoved {
			t.RemovedCols[op.Column] = struct{}{}
		}
		if op.RowWasRemoved {
			t.RemovedRows[op.Row] = struct{}{}
		}
	}
	t.SelectableTable.Select((op.Row+1)*(t.cd.Layers()+1), op.Column+1, 0)
	t.redoStack.PushFront(op)
}

func (t *MergeTable) redo() {
	e := t.redoStack.Front()
	if e == nil {
		return
	}
	t.redoStack.Remove(e)
	op := e.Value.(*editOp)
	t.execOp(op)
	t.SelectableTable.Select((op.Row+1)*(t.cd.Layers()+1), op.Column+1, 0)
}

func (t *MergeTable) edit(op *editOp) {
	// modify edit op before it is carried out
	if op.Type == editSet {
		if _, ok := t.RemovedCols[op.Column]; ok {
			op.ColWasRemoved = true
		}
		if _, ok := t.RemovedRows[op.Row]; ok {
			op.RowWasRemoved = true
		}
	}
	t.execOp(op)
	t.redoStack = t.redoStack.Init()
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
		oldVal, different := t.rowPool.IsTextAtCellDifferentFromBase(mergeInd, column, row)
		if different {
			t.edit(&editOp{
				Type:   editSet,
				Row:    mergeInd,
				Layer:  row,
				Column: column - 1,
				OldVal: oldVal,
			})
		}
	}
}

func (t *MergeTable) deleteColumn() {
	_, selectedCol, _ := t.SelectableTable.GetSelection()
	if selectedCol <= len(t.cd.OtherPK[0]) || selectedCol > t.cd.Len() {
		return
	}
	t.edit(&editOp{
		Type:   editRemoveCol,
		Column: selectedCol - 1,
	})
}

func (t *MergeTable) deleteRow() {
	selectedRow, _, _ := t.SelectableTable.GetSelection()
	rowCount, _ := t.GetShape()
	if selectedRow <= 0 || selectedRow >= rowCount {
		return
	}
	mergeInd, row := t.mergeRowAtRow(selectedRow)
	if row == t.cd.Layers() {
		t.edit(&editOp{
			Type: editRemoveRow,
			Row:  mergeInd,
		})
	}
}

func (t *MergeTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return t.SelectableTable.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) bool {
		key := event.Key()
		switch key {
		case tcell.KeyRune:
			switch event.Rune() {
			case 'u':
				t.undo()
			case 'U':
				t.redo()
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

func MergeTableUsageBar() *UsageBar {
	return NewUsageBar([][2]string{
		{"g", "Scroll to begin"},
		{"G", "Scroll to end"},
		{"h", "Left"},
		{"j", "Down"},
		{"k", "Up"},
		{"l", "Right"},
		{"u", "Undo"},
		{"U", "Redo"},
		{"d", "Delete row"},
		{"D", "Delete column"},
	}, 2)
}
