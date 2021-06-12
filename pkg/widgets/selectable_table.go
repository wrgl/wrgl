package widgets

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// SelectableTable add following behaviors on top of VirtualTable:
// - Allow selecting cell and row (by selecting row number cell)
// - Define default selected style
type SelectableTable struct {
	*VirtualTable

	// This function is called to get the underlying table cell at any position
	getCells func(row, column int) []*TableCell

	// The currently selected row and column.
	selectedRow, selectedColumn, selectedSubColumn int

	// Minimum row and column index which can be selected.
	minSelectedRow, minSelectedCol int

	// An optional function which gets called when the user presses Enter on a
	// selected cell. If entire rows selected, the column value is undefined.
	// Likewise for entire columns.
	selected func(row, column, subColumn int)
}

func NewSelectableTable() *SelectableTable {
	t := &SelectableTable{
		VirtualTable: NewVirtualTable(),
	}
	t.VirtualTable.SetGetCellsFunc(t.getStyledCells)
	return t
}

// SetGetCellsFunc set function to get table cell at a position
func (t *SelectableTable) SetGetCellsFunc(getCells func(row, column int) []*TableCell) *SelectableTable {
	t.getCells = getCells
	return t
}

// SetSelectedFunc sets a handler which is called whenever the user presses the
// Enter key on a selected cell/row/column. The handler receives the position of
// the selection and its cell contents. If entire rows are selected, the column
// index is undefined. Likewise for entire columns.
func (t *SelectableTable) SetSelectedFunc(handler func(row, column, subColumn int)) *SelectableTable {
	t.selected = handler
	return t
}

// SetMinSelection tells table to prevent selection from going below certain index
func (t *SelectableTable) SetMinSelection(row, column int) *SelectableTable {
	t.minSelectedRow, t.minSelectedCol = row, column
	return t
}

// Select sets the selected cell. Depending on the selection settings
// specified via SetSelectable(), this may be an entire row or column, or even
// ignored completely. The "selection changed" event is fired if such a callback
// is available (even if the selection ends up being the same as before and even
// if cells are not selectable).
func (t *SelectableTable) Select(row, column, subCol int) *SelectableTable {
	t.selectedRow, t.selectedColumn, t.selectedSubColumn = row, column, subCol
	return t
}

func (t *SelectableTable) Draw(screen tcell.Screen) {
	t.Box.DrawForSubclass(screen, t)
	_, _, _, height := t.Box.GetInnerRect()

	// Clamp offsets.
	if t.selectedRow >= t.fixedRows && t.selectedRow < t.fixedRows+t.rowOffset {
		t.rowOffset = t.selectedRow - t.fixedRows
	}
	if t.borders {
		if 2*(t.selectedRow+1-t.rowOffset) >= height {
			t.rowOffset = t.selectedRow + 1 - height/2
		}
	} else {
		if t.selectedRow+1-t.rowOffset >= height {
			t.rowOffset = t.selectedRow + 1 - height
		}
	}
	if t.selectedColumn < t.columnOffset {
		t.columnOffset = t.selectedColumn
	}
	t.VirtualTable.KeepColumnInView(t.selectedColumn).
		Draw(screen)
}

func (t *SelectableTable) getStyledCells(row, column int) []*TableCell {
	if row < 0 || column < 0 || row >= t.rowCount || column >= t.columnCount {
		return nil
	}
	cells := t.getCells(row, column)
	if cells == nil {
		return nil
	}
	for i, cell := range cells {
		if row == t.selectedRow && (t.selectedColumn == 0 ||
			(t.selectedColumn == column && (t.selectedSubColumn == i || len(cells) == 1))) {
			cell.SetFlipped(true).SetTransparency(false)
		} else {
			cell.SetFlipped(false).SetTransparency(true)
		}
	}
	return cells
}

func (t *SelectableTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return t.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		key := event.Key()
		rowCount, columnCount := t.VirtualTable.GetShape()

		var (
			updateSubCol = func(num int) {
				t.selectedSubColumn = num
				cells := t.getCells(t.selectedRow, t.selectedColumn)
				if len(cells) == 1 {
					t.selectedSubColumn = 0
				}
			}

			home = func() {
				t.selectedRow = t.minSelectedRow
				t.selectedColumn = t.minSelectedCol
				t.selectedSubColumn = 0
			}

			end = func() {
				t.selectedRow = rowCount - 1
				t.selectedColumn = columnCount - 1
				t.selectedSubColumn = 0
			}

			down = func() {
				t.selectedRow++
				if t.selectedRow >= rowCount {
					t.selectedRow = rowCount - 1
				}
				updateSubCol(t.selectedSubColumn)
			}

			up = func() {
				t.selectedRow--
				if t.selectedRow < t.minSelectedRow {
					t.selectedRow = t.minSelectedRow
				}
				updateSubCol(t.selectedSubColumn)
			}

			left = func() {
				if t.selectedSubColumn > 0 {
					t.selectedSubColumn--
				} else {
					t.selectedColumn--
					if t.selectedColumn < t.minSelectedCol {
						t.selectedColumn = t.minSelectedCol
					}
					updateSubCol(1)
				}
			}

			right = func() {
				cells := t.getCells(t.selectedRow, t.selectedColumn)
				if t.selectedSubColumn < len(cells)-1 {
					t.selectedSubColumn++
				} else {
					t.selectedSubColumn = 0
					t.selectedColumn++
					if t.selectedColumn >= columnCount {
						t.selectedColumn = columnCount - 1
					}
				}
			}

			pageDown = func() {
				offsetAmount := t.visibleRows - t.fixedRows
				if offsetAmount < 0 {
					offsetAmount = 0
				}

				t.selectedRow += offsetAmount
				if t.selectedRow >= rowCount {
					t.selectedRow = rowCount - 1
				}
				updateSubCol(t.selectedSubColumn)
			}

			pageUp = func() {
				offsetAmount := t.visibleRows - t.fixedRows
				if offsetAmount < 0 {
					offsetAmount = 0
				}

				t.selectedRow -= offsetAmount
				if t.selectedRow < t.minSelectedRow {
					t.selectedRow = t.minSelectedRow
				}
				updateSubCol(t.selectedSubColumn)
			}
		)

		switch key {
		case tcell.KeyRune:
			switch event.Rune() {
			case 'g':
				home()
			case 'G':
				end()
			case 'j':
				down()
			case 'k':
				up()
			case 'h':
				left()
			case 'l':
				right()
			}
		case tcell.KeyHome:
			home()
		case tcell.KeyEnd:
			end()
		case tcell.KeyUp:
			up()
		case tcell.KeyDown:
			down()
		case tcell.KeyLeft:
			left()
		case tcell.KeyRight:
			right()
		case tcell.KeyPgDn, tcell.KeyCtrlF:
			pageDown()
		case tcell.KeyPgUp, tcell.KeyCtrlB:
			pageUp()
		case tcell.KeyEnter:
			if t.selected != nil {
				t.selected(t.selectedRow, t.selectedColumn, t.selectedSubColumn)
			}
		}
	})
}

// MouseHandler returns the mouse handler for this primitive.
func (t *SelectableTable) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
	return t.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
		x, y := event.Position()
		if !t.InRect(x, y) {
			return false, nil
		}

		switch action {
		case tview.MouseLeftClick:
			rowCount, columnCount := t.VirtualTable.GetShape()
			selectEvent := true
			row, column, subCol := t.cellAt(x, y)
			if row >= 0 && row < rowCount && column >= 0 && column < columnCount {
				cells := t.getStyledCells(row, column)
				var cell *TableCell
				if len(cells) == 1 {
					cell = cells[0]
				} else {
					cell = cells[subCol]
				}
				if cell != nil && cell.Clicked != nil {
					if noSelect := cell.Clicked(); noSelect {
						selectEvent = false
					}
				}
			}
			if selectEvent && row >= t.minSelectedRow && column >= t.minSelectedCol {
				t.Select(row, column, subCol)
			}
			setFocus(t)
			consumed = true
		default:
			return t.VirtualTable.MouseHandler()(action, event, setFocus)
		}

		return
	})
}
