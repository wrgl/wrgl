// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package widgets

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/merge"
)

const (
	editSet int = iota
	editRemoveCol
	editRemoveRow
	editResolveRow
	editUnresolveRow
)

type editOp struct {
	Type            int
	Row             int
	Column          int
	Value           string
	OldVal          string
	ColWasRemoved   bool
	RowWasRemoved   bool
	CellWasResolved bool
	RowWasResolved  bool
	AffectedRows    []int
}

type MergeApp struct {
	buf          *diff.BlockBuffer
	merger       *merge.Merger
	cd           *diff.ColDiff
	merges       []*merge.Merge
	RemovedCols  map[int]struct{}
	removedRows  map[int]struct{}
	undoStack    *list.List
	redoStack    *list.List
	commitNames  []string
	commitSums   [][]byte
	app          *tview.Application
	Table        *MergeTable
	Flex         *tview.Flex
	statusBar    *tview.TextView
	resolvedRows map[int]struct{}
	usageBar     *UsageBar
	inputField   *tview.InputField
	inputRow     int
	inputColumn  int
	Finished     bool
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

func NewMergeApp(buf *diff.BlockBuffer, merger *merge.Merger, app *tview.Application, commitNames []string, commitSums [][]byte, baseSum []byte) *MergeApp {
	return &MergeApp{
		buf:          buf,
		merger:       merger,
		app:          app,
		RemovedCols:  map[int]struct{}{},
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

func (a *MergeApp) updateStatus() {
	a.statusBar.Clear()
	n := len(a.merges)
	resolved := len(a.resolvedRows)
	pct := float32(resolved) / float32(n) * 100
	statText := fmt.Sprintf("Resolved %d / %d rows (%.1f%%) - ", resolved, n, pct)

	colStats := []string{}
	names := []string{}
	_, _, column := a.Table.GetSelection()
	for i, m := range a.cd.Added {
		if _, ok := m[uint32(column)]; ok {
			names = append(names, fmt.Sprintf(
				"[yellow]%s[white]",
				hex.EncodeToString(a.commitSums[i])[:7],
			))
		}
	}
	if len(names) == 1 {
		colStats = append(colStats, fmt.Sprintf("added in %s", names[0]))
	} else if len(names) > 1 {
		colStats = append(colStats, fmt.Sprintf("added in %s", strings.Join(names, ", ")))
	}
	names = names[:0]
	for i, m := range a.cd.Removed {
		if _, ok := m[uint32(column)]; ok {
			names = append(names, fmt.Sprintf(
				"[yellow]%s[white]",
				hex.EncodeToString(a.commitSums[i])[:7],
			))
		}
	}
	if len(names) == 1 {
		colStats = append(colStats, fmt.Sprintf("removed in %s", names[0]))
	} else if len(names) > 1 {
		colStats = append(colStats, fmt.Sprintf("removed in %s", strings.Join(names, ", ")))
	}
	if len(colStats) > 0 {
		statText = fmt.Sprintf("%scolumn %s", statText, strings.Join(colStats, ", "))
	}
	fmt.Fprint(a.statusBar, statText)
}

func (a *MergeApp) InitializeTable(cd *diff.ColDiff, merges []*merge.Merge) {
	a.cd = cd
	a.merges = merges
	a.Table = NewMergeTable(a.buf, a.commitNames, a.commitSums, a.cd, a.merges, a.RemovedCols, a.removedRows).
		SetUndoHandler(a.undo).
		SetRedoHandler(a.redo).
		SetResolveHandler(a.resolveRow).
		SetUnresolveHandler(a.unresolveRow).
		SetSetCellHandler(a.setCellFromLayer).
		SetDeleteColumnHandler(a.deleteColumn).
		SetDeleteRowHandler(a.deleteRow).
		SetSelectNextConflict(a.selectNextConflict).
		SetShowInputHandler(a.showInput).
		SetAbortHandler(a.abort).
		SetFinishHandler(a.finish)
	a.Table.SetSelectionChangedFunc(a.updateStatus)
	a.statusBar = tview.NewTextView().SetDynamicColors(true)
	a.updateStatus()
	a.inputField = tview.NewInputField().
		SetLabel("Enter cell's value: ").
		SetDoneFunc(a.handleInput)
	a.inputField.SetBorderPadding(1, 1, 0, 0)
	a.usageBar = NewUsageBar([][2]string{
		{"n", "Next conflict"},
		{"r", "Mark row as resolved"},
		{"R", "Mark row as unresolved"},
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
		{"Q", "Abort merge"},
		{"X", "Finish merge"},
	}, 2)
	a.Flex.AddItem(a.statusBar, 1, 1, false).
		AddItem(a.Table, 0, 1, true).
		AddItem(a.inputField, 0, 0, false).
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
		a.RemovedCols[op.Column] = struct{}{}
		for _, i := range op.AffectedRows {
			m := a.merges[i]
			cols := m.UnresolvedCols
			delete(cols, uint32(op.Column))
			if len(cols) == 0 {
				a.resolvedRows[i] = struct{}{}
				m.Resolved = true
				a.Table.RefreshRow(i)
			}
		}
	case editRemoveRow:
		a.removedRows[op.Row] = struct{}{}
		a.resolvedRows[op.Row] = struct{}{}
		a.merges[op.Row].Resolved = true
		a.Table.RefreshRow(op.Row)
	case editSet:
		delete(a.RemovedCols, op.Column)
		delete(a.removedRows, op.Row)
		m := a.merges[op.Row]
		m.ResolvedRow[op.Column] = op.Value
		unresolvedCols := m.UnresolvedCols
		if unresolvedCols != nil {
			delete(unresolvedCols, uint32(op.Column))
			if len(unresolvedCols) == 0 {
				a.resolvedRows[op.Row] = struct{}{}
				a.merges[op.Row].Resolved = true
				a.Table.RefreshRow(op.Row)
			}
		}
		a.Table.SetCell(op.Row, op.Column, m.ResolvedRow[op.Column], false)
	case editResolveRow:
		a.resolvedRows[op.Row] = struct{}{}
		a.merges[op.Row].Resolved = true
		a.Table.RefreshRow(op.Row)
	case editUnresolveRow:
		delete(a.resolvedRows, op.Row)
		a.merges[op.Row].Resolved = false
		a.Table.RefreshRow(op.Row)
	}
	a.undoStack.PushFront(op)
	a.updateStatus()
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
		delete(a.RemovedCols, op.Column)
		for _, i := range op.AffectedRows {
			cols := a.merges[i].UnresolvedCols
			cols[uint32(op.Column)] = struct{}{}
			delete(a.resolvedRows, i)
			a.merges[i].Resolved = false
			a.Table.RefreshRow(i)
		}
	case editRemoveRow:
		delete(a.removedRows, op.Row)
		if op.RowWasResolved {
			delete(a.resolvedRows, op.Row)
			a.merges[op.Row].Resolved = false
			a.Table.RefreshRow(op.Row)
		}
	case editSet:
		if op.ColWasRemoved {
			a.RemovedCols[op.Column] = struct{}{}
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
				a.merges[op.Row].Resolved = false
				a.Table.RefreshRow(op.Row)
			}
		}
		a.Table.SetCell(op.Row, op.Column, op.OldVal, unresolved)
	case editResolveRow:
		delete(a.resolvedRows, op.Row)
		a.merges[op.Row].Resolved = false
		a.Table.RefreshRow(op.Row)
	case editUnresolveRow:
		a.resolvedRows[op.Row] = struct{}{}
		a.merges[op.Row].Resolved = true
		a.Table.RefreshRow(op.Row)
	}
	a.Table.Select(op.Row, op.Column)
	a.redoStack.PushFront(op)
	a.updateStatus()
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

func (a *MergeApp) setCellFromLayer(row, column, layer int) {
	a.setCellWithValue(row, column, a.Table.GetCellText(row, column, layer))
}

func (a *MergeApp) setCellWithValue(row, column int, value string) {
	a.setCell(&editOp{
		Type:   editSet,
		Row:    row,
		Value:  value,
		Column: column,
	})
}

func (a *MergeApp) setCell(op *editOp) {
	if _, ok := a.RemovedCols[op.Column]; ok {
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
			if _, ok := a.resolvedRows[op.Row]; !ok && len(unresolvedCols) == 1 {
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

func (a *MergeApp) resolveRow(row int) {
	if _, ok := a.resolvedRows[row]; ok {
		return
	}
	op := &editOp{
		Type: editResolveRow,
		Row:  row,
	}
	a.edit(op)
}

func (a *MergeApp) unresolveRow(row int) {
	if _, ok := a.resolvedRows[row]; !ok {
		return
	}
	op := &editOp{
		Type: editUnresolveRow,
		Row:  row,
	}
	a.edit(op)
}

func (a *MergeApp) selectNextConflict() {
mainLoop:
	for i, m := range a.merges {
		if _, ok := a.resolvedRows[i]; ok {
			continue
		}
		for j := range m.UnresolvedCols {
			a.Table.Select(i, int(j))
			break mainLoop
		}
		a.Table.Select(i, 0)
		break
	}
	a.updateStatus()
}

func (a *MergeApp) handleInput(key tcell.Key) {
	switch key {
	case tcell.KeyEnter:
		a.setCellWithValue(a.inputRow, a.inputColumn, a.inputField.GetText())
		a.Flex.ResizeItem(a.inputField, 0, 0)
		a.app.SetFocus(a.Table)
	case tcell.KeyEscape:
		a.Flex.ResizeItem(a.inputField, 0, 0)
		a.app.SetFocus(a.Table)
	}
}

func (a *MergeApp) showInput(row, column int) {
	a.inputRow = row
	a.inputColumn = column
	a.inputField.SetText(a.merges[row].ResolvedRow[column])
	a.Flex.ResizeItem(a.inputField, 3, 1)
	a.app.SetFocus(a.inputField)
}

func (a *MergeApp) abort() {
	a.app.Stop()
}

func (a *MergeApp) saveResolvedRows() {
	for _, m := range a.merges {
		if err := a.merger.SaveResolvedRow(m.PK, m.ResolvedRow); err != nil {
			a.app.Stop()
			panic(err)
		}
	}
	a.Finished = true
	a.app.Stop()
}

func (a *MergeApp) finish() {
	if len(a.resolvedRows) < len(a.merges) {
		modal := tview.NewModal().
			SetText(fmt.Sprintf(
				"There are still %d conflicts not yet resolved. Do you want to finish merging? (conflicting rows will be saved as is)",
				len(a.merges)-len(a.resolvedRows),
			)).
			AddButtons([]string{"Finish", "Cancel"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				switch buttonLabel {
				case "Finish":
					a.saveResolvedRows()
				case "Cancel":
					a.app.SetRoot(a.Flex, true)
				}
			})
		a.app.SetRoot(modal, true)
	} else {
		a.saveResolvedRows()
	}
}
