package widgets

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
)

const (
	editSet int = iota
	editRemoveCol
	editRemoveRow
)

type editOp struct {
	Type            int
	Row             int
	Layer           int
	Column          int
	OldVal          string
	ColWasRemoved   bool
	RowWasRemoved   bool
	CellWasResolved bool
	RowWasResolved  bool
	AffectedRows    []int
}

type MergeApp struct {
	db           kv.DB
	fs           kv.FileStore
	merger       *merge.Merger
	cd           *objects.ColDiff
	merges       []*merge.Merge
	removedCols  map[int]struct{}
	removedRows  map[int]struct{}
	undoStack    *list.List
	redoStack    *list.List
	commitNames  []string
	commitSums   [][]byte
	Table        *MergeTable
	Flex         *tview.Flex
	statusBar    *tview.TextView
	resolvedRows map[int]struct{}
	usageBar     *UsageBar
}

func createMergeTitleBar(commitNames []string, baseSum []byte) *tview.TextView {
	titleBar := tview.NewTextView().SetDynamicColors(true)
	sl := make([]string, len(commitNames))
	for i, s := range commitNames {
		sl[i] = fmt.Sprintf("[yellow]%s[white]", s)
	}
	fmt.Fprintf(
		titleBar, "Merging %s (base [yellow]%s[white])", strings.Join(sl, ", "), hex.EncodeToString(baseSum)[:7],
	)
	return titleBar
}

func NewMergeApp(db kv.DB, fs kv.FileStore, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte) *MergeApp {
	return &MergeApp{
		db:           db,
		fs:           fs,
		merger:       merger,
		removedCols:  map[int]struct{}{},
		removedRows:  map[int]struct{}{},
		resolvedRows: map[int]struct{}{},
		undoStack:    list.New(),
		redoStack:    list.New(),
		commitNames:  commitNames,
		commitSums:   commitSums,
		Flex: tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(createMergeTitleBar(commitNames, baseSum), 1, 1, false),
	}
}

func (a *MergeApp) CollectMergeConflicts() error {
	pBar := NewProgressBar("Counting merge conflicts...")
	a.Flex.AddItem(pBar, 1, 1, false)
	mch, err := a.merger.Start()
	if err != nil {
		return err
	}
	pch := a.merger.Progress.Chan()
	go a.merger.Progress.Run()
	a.merges = []*merge.Merge{}
mainLoop:
	for {
		select {
		case p := <-pch:
			pBar.SetTotal(p.Total)
			pBar.SetCurrent(p.Progress)
		case m, ok := <-mch:
			if !ok {
				break mainLoop
			}
			a.merges = append(a.merges, m)
		}
	}
	a.merger.Progress.Stop()
	a.Flex.RemoveItem(pBar)
	if err = a.merger.Error(); err != nil {
		return err
	}
	a.cd = a.merges[0].ColDiff
	a.merges = a.merges[1:]
	return nil
}

func (a *MergeApp) updateStatus(s string) {
	a.statusBar.Clear()
	n := len(a.merges)
	resolved := len(a.resolvedRows)
	pct := float32(resolved) / float32(n) * 100
	fmt.Fprintf(a.statusBar, "Resolved %d / %d rows (%.1f%%) - %s", resolved, n, pct, s)
}

func (a *MergeApp) InitializeTable() {
	a.Table = NewMergeTable(a.db, a.fs, a.commitNames, a.commitSums, a.cd, a.merges, a.removedCols, a.removedRows).
		SetUndoHandler(a.undo).
		SetRedoHandler(a.redo).
		SetSetCellHandler(a.setCell).
		SetDeleteColumnHandler(a.deleteColumn).
		SetDeleteRowHandler(a.deleteRow).
		SetSelectNextConflict(a.selectNextConflict)
	a.statusBar = tview.NewTextView().SetDynamicColors(true)
	a.updateStatus("")
	a.usageBar = NewUsageBar([][2]string{
		{"n", "Next conflict"},
		{"u", "Undo"},
		{"U", "Redo"},
		{"d", "Delete row"},
		{"D", "Delete column"},
		{"h", "Left"},
		{"j", "Down"},
		{"k", "Up"},
		{"l", "Right"},
		{"g", "Scroll to begin"},
		{"G", "Scroll to end"},
	}, 2)
	a.Flex.AddItem(a.statusBar, 1, 1, false).
		AddItem(a.Table, 0, 1, true).
		AddItem(a.usageBar, 1, 1, false)
}

func (a *MergeApp) BeforeDraw(screen tcell.Screen) {
	if a.usageBar != nil {
		a.usageBar.BeforeDraw(screen, a.Flex)
	}
}

func (a *MergeApp) execOp(op *editOp) {
	switch op.Type {
	case editRemoveCol:
		a.removedCols[op.Column] = struct{}{}
		for _, i := range op.AffectedRows {
			cols := a.merges[i].UnresolvedCols
			delete(cols, uint32(op.Column))
			if len(cols) == 0 {
				a.resolvedRows[i] = struct{}{}
			}
		}
	case editRemoveRow:
		a.removedRows[op.Row] = struct{}{}
		a.resolvedRows[op.Row] = struct{}{}
	case editSet:
		delete(a.removedCols, op.Column)
		delete(a.removedRows, op.Row)
		m := a.merges[op.Row]
		m.ResolvedRow[op.Column] = a.Table.GetCellText(op.Row, op.Column, op.Layer)
		unresolvedCols := m.UnresolvedCols
		if unresolvedCols != nil {
			delete(unresolvedCols, uint32(op.Column))
			if len(unresolvedCols) == 0 {
				a.resolvedRows[op.Row] = struct{}{}
			}
		}
		a.Table.SetCell(op.Row, op.Column, m.ResolvedRow[op.Column], false)
	}
	a.undoStack.PushFront(op)
	a.updateStatus("")
}

func (a *MergeApp) undo() {
	e := a.undoStack.Front()
	if e == nil {
		return
	}
	a.undoStack.Remove(e)
	op := e.Value.(*editOp)
	switch op.Type {
	case editRemoveCol:
		delete(a.removedCols, op.Column)
		for _, i := range op.AffectedRows {
			cols := a.merges[i].UnresolvedCols
			cols[uint32(op.Column)] = struct{}{}
			delete(a.resolvedRows, i)
		}
	case editRemoveRow:
		delete(a.removedRows, op.Row)
		if op.RowWasResolved {
			delete(a.resolvedRows, op.Row)
		}
	case editSet:
		if op.ColWasRemoved {
			a.removedCols[op.Column] = struct{}{}
		}
		if op.RowWasRemoved {
			a.removedRows[op.Row] = struct{}{}
		}
		unresolved := false
		m := a.merges[op.Row]
		m.ResolvedRow[op.Column] = op.OldVal
		if op.CellWasResolved {
			m.UnresolvedCols[uint32(op.Column)] = struct{}{}
			unresolved = true
			if op.RowWasResolved {
				delete(a.resolvedRows, op.Row)
			}
		}
		a.Table.SetCell(op.Row, op.Column, op.OldVal, unresolved)
	}
	a.Table.Select(op.Row, op.Column)
	a.redoStack.PushFront(op)
	a.updateStatus("")
}

func (a *MergeApp) redo() {
	e := a.redoStack.Front()
	if e == nil {
		return
	}
	a.redoStack.Remove(e)
	op := e.Value.(*editOp)
	a.execOp(op)
	a.Table.Select(op.Row, op.Column)
}

func (a *MergeApp) edit(op *editOp) {
	a.execOp(op)
	a.redoStack = a.redoStack.Init()
}

func (a *MergeApp) setCell(row, column, layer int) {
	op := &editOp{
		Type:   editSet,
		Row:    row,
		Layer:  layer,
		Column: column,
	}
	if _, ok := a.removedCols[op.Column]; ok {
		op.ColWasRemoved = true
	}
	if _, ok := a.removedRows[op.Row]; ok {
		op.RowWasRemoved = true
	}
	op.OldVal = a.Table.GetCellText(op.Row, op.Column, a.cd.Layers())
	unresolvedCols := a.merges[op.Row].UnresolvedCols
	if unresolvedCols != nil {
		if _, ok := unresolvedCols[uint32(op.Column)]; ok {
			op.CellWasResolved = true
			if len(unresolvedCols) == 1 {
				op.RowWasResolved = true
			}
		}
	}
	a.edit(op)
}

func (a *MergeApp) deleteColumn(column int) {
	op := &editOp{
		Type:   editRemoveCol,
		Column: column,
	}
	for i, m := range a.merges {
		if _, ok := m.UnresolvedCols[uint32(column)]; ok {
			op.AffectedRows = append(op.AffectedRows, i)
		}
	}
	a.edit(op)
}

func (a *MergeApp) deleteRow(row int) {
	op := &editOp{
		Type: editRemoveRow,
		Row:  row,
	}
	if _, ok := a.resolvedRows[row]; !ok {
		op.RowWasResolved = true
	}
	a.edit(op)
}

func (a *MergeApp) selectNextConflict() {
	for i, m := range a.merges {
		if _, ok := a.resolvedRows[i]; ok {
			continue
		}
		for j := range m.UnresolvedCols {
			a.Table.Select(i, int(j))
			return
		}
	}
}
