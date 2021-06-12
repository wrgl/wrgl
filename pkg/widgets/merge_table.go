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
	"github.com/wrgl/core/pkg/table"
)

var (
	boldStyle       = tcell.StyleDefault.Background(tcell.ColorBlack).Bold(true)
	boldRedStyle    = boldStyle.Foreground(tcell.ColorRed)
	boldGreenStyle  = boldStyle.Foreground(tcell.ColorGreen)
	boldYellowStyle = boldStyle.Foreground(tcell.ColorYellow)
	yellowStyle     = cellStyle.Foreground(tcell.ColorYellow)
	greenStyle      = cellStyle.Foreground(tcell.ColorGreen)
	redStyle        = cellStyle.Foreground(tcell.ColorRed)
	deletedCell     = NewTableCell("REMOVED").SetStyle(redStyle)
)

const (
	editSet int = iota
	editRemoveCol
)

type editOp struct {
	Type          int
	Row           int
	Column        int
	OldVal        string
	ColWasRemoved bool
}

type MergeTable struct {
	*SelectableTable

	db                 kv.DB
	cd                 *objects.ColDiff
	dec                *objects.StrListDecoder
	columnCells        []*TableCell
	cells              [][]*TableCell
	commitCells        []*TableCell
	PK                 []byte
	RemovedCols        map[int]struct{}
	baseRow            []string
	undoStack          *list.List
	redoStack          *list.List
	resolvedRowHandler func([]string)
}

func NewMergeTable(db kv.DB, fs kv.FileStore, commitNames []string, commitSums [][]byte, baseSum []byte, cd *objects.ColDiff, resolvedRowHandler func([]string)) (*MergeTable, error) {
	n := len(commitNames)
	t := &MergeTable{
		db:                 db,
		SelectableTable:    NewSelectableTable(),
		cells:              make([][]*TableCell, n+1),
		commitCells:        make([]*TableCell, n+1),
		dec:                objects.NewStrListDecoder(false),
		cd:                 cd,
		RemovedCols:        map[int]struct{}{},
		undoStack:          list.New(),
		redoStack:          list.New(),
		resolvedRowHandler: resolvedRowHandler,
	}
	for i, name := range commitNames {
		t.commitCells[i] = t.commitCell(name, commitSums[i])
	}
	t.commitCells[len(t.commitCells)-1] = t.commitCell("", baseSum)
	t.SelectableTable.SetGetCellsFunc(t.getMergeCells).
		SetSelectedFunc(t.selectCell).
		SetMinSelection(1, 1).
		Select(1, 1, 0).
		SetFixed(1, 1).
		SetBorders(true)
	err := t.createCells()
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *MergeTable) commitCell(name string, sum []byte) *TableCell {
	var txt string
	if name == "" {
		txt = fmt.Sprintf("base (%s)", hex.EncodeToString(sum)[:7])
	} else {
		txt = fmt.Sprintf("%s (%s)", name, hex.EncodeToString(sum)[:7])
	}
	return NewTableCell(txt).SetStyle(boldStyle)
}

func (t *MergeTable) createCells() error {
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
	numPK := len(t.cd.OtherPK[0])
	for i := 0; i < t.cd.Layers()+1; i++ {
		t.cells[i] = make([]*TableCell, numCols)
		for j := 0; j < numCols; j++ {
			t.cells[i][j] = NewTableCell("").SetStyle(cellStyle)
			if j < numPK {
				t.cells[i][j].SetStyle(primaryKeyStyle)
			}
		}
	}
	t.SelectableTable.SetShape(t.cd.Layers()+2, numCols+1)
	if numPK > 0 {
		t.SelectableTable.SetMinSelection(1, numPK+1).
			Select(1, numPK+1, 0).
			SetFixed(1, numPK+1)
	}
	return nil
}

func (t *MergeTable) resolvedRow() []string {
	cells := t.cells[len(t.cells)-1]
	res := make([]string, len(cells))
	for i, cell := range cells {
		res[i] = cell.Text
	}
	return res
}

func (t *MergeTable) ShowMerge(m *merge.Merge) error {
	t.undoStack = t.undoStack.Init()
	t.redoStack = t.redoStack.Init()
	t.PK = m.PK

	baseInd := len(t.cells) - 1
	t.baseRow = make([]string, t.cd.Len())
	if m.Base != nil {
		rowB, err := table.GetRow(t.db, m.Base)
		if err != nil {
			return err
		}
		row := t.cd.RearrangeBaseRow(t.dec.Decode(rowB))
		for i, s := range row {
			t.cells[baseInd][i].SetText(s)
			t.baseRow[i] = s
		}
		t.commitCells[baseInd].SetStyle(boldStyle)
	} else {
		t.commitCells[baseInd].SetStyle(boldRedStyle)
	}

	for i, sum := range m.Others {
		cell := t.commitCells[i]
		if sum == nil {
			cell.SetStyle(boldRedStyle)
			continue
		} else if m.Base == nil {
			cell.SetStyle(boldGreenStyle)
		} else if string(sum) == string(m.Base) {
			cell.SetStyle(boldStyle)
		} else {
			cell.SetStyle(boldYellowStyle)
		}
		rowB, err := table.GetRow(t.db, sum)
		if err != nil {
			return err
		}
		row := t.cd.RearrangeRow(i, t.dec.Decode(rowB))
		for j, s := range row {
			cell := t.cells[i][j]
			cell.SetText(s)
			if j >= len(t.cd.OtherPK[0]) {
				baseTxt := t.baseRow[j]
				if _, ok := t.cd.Removed[i][uint32(j)]; ok {
					cell.SetStyle(redStyle)
				} else if _, ok := t.cd.Added[i][uint32(j)]; ok {
					cell.SetStyle(greenStyle)
				} else if baseTxt != cell.Text {
					cell.SetStyle(yellowStyle)
				} else {
					cell.SetStyle(cellStyle)
				}
			}
		}
	}
	return nil
}

func (t *MergeTable) getMergeCells(row, column int) []*TableCell {
	if row == 0 {
		if column == 0 {
			return []*TableCell{NewTableCell("")}
		}
		return []*TableCell{t.columnCells[column-1]}
	}
	if column == 0 {
		return []*TableCell{t.commitCells[row-1]}
	}
	cell := t.cells[row-1][column-1]
	if column > len(t.cd.OtherPK[0]) && row-1 == t.cd.Layers() {
		if _, ok := t.RemovedCols[column-1]; ok {
			cell = deletedCell
		}
	}
	return []*TableCell{cell}
}

func (t *MergeTable) setModifiedCellStyle(col int) {
	cell := t.cells[t.cd.Layers()][col]
	txt := cell.Text
	for i, m := range t.cd.Added {
		if _, ok := m[uint32(col)]; ok && t.cells[i][col].Text == txt {
			cell.SetStyle(greenStyle)
			return
		}
	}
	if txt != t.baseRow[col] {
		cell.SetStyle(yellowStyle)
	} else {
		cell.SetStyle(cellStyle)
	}
}

func (t *MergeTable) execOp(op *editOp) {
	nLayers := t.cd.Layers()
	switch op.Type {
	case editRemoveCol:
		t.RemovedCols[op.Column] = struct{}{}
	case editSet:
		delete(t.RemovedCols, op.Column)
		t.cells[nLayers][op.Column].SetText(t.cells[op.Row][op.Column].Text)
		t.setModifiedCellStyle(op.Column)
	}
	t.undoStack.PushFront(op)
}

func (t *MergeTable) undo() {
	e := t.undoStack.Front()
	if e == nil {
		return
	}
	t.undoStack.Remove(e)
	nLayers := t.cd.Layers()
	op := e.Value.(*editOp)
	cell := t.cells[nLayers][op.Column]
	switch op.Type {
	case editRemoveCol:
		delete(t.RemovedCols, op.Column)
		cell.SetText(op.OldVal)
	case editSet:
		cell.SetText(op.OldVal)
		if op.ColWasRemoved {
			t.RemovedCols[op.Column] = struct{}{}
		}
		t.setModifiedCellStyle(op.Column)
	}
	t.SelectableTable.Select(nLayers+1, op.Column+1, 0)
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
	t.SelectableTable.Select(t.cd.Layers()+1, op.Column+1, 0)
}

func (t *MergeTable) edit(op *editOp) {
	t.execOp(op)
	t.redoStack = t.redoStack.Init()
}

func (t *MergeTable) selectCell(row, column, subCol int) {
	nLayers := t.cd.Layers()
	if row <= 0 || row > nLayers+1 || column <= len(t.cd.OtherPK[0]) || column > t.cd.Len() {
		return
	}
	if row == nLayers+1 {

	} else {
		if t.cells[row-1][column-1].Text != t.cells[nLayers][column-1].Text {
			_, ok := t.RemovedCols[column-1]
			t.edit(&editOp{
				Type:          editSet,
				Row:           row - 1,
				Column:        column - 1,
				OldVal:        t.cells[nLayers][column-1].Text,
				ColWasRemoved: ok,
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
		OldVal: t.cells[t.cd.Layers()][selectedCol-1].Text,
	})
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
			case 'D':
				t.deleteColumn()
			case 'S':
				t.resolvedRowHandler(t.resolvedRow())
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
		{"D", "Delete column"},
		{"S", "Save row"},
	}, 2)
}
