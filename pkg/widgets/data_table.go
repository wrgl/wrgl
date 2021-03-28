package widgets

import (
	"strconv"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

var (
	columnStyle     = tcell.StyleDefault.Foreground(tcell.ColorAzure).Bold(true)
	rowCountStyle   = tcell.StyleDefault.Foreground(tcell.ColorSlateGray)
	primaryKeyStyle = tcell.StyleDefault.Foreground(tcell.ColorAquaMarine).Background(tcell.ColorBlack)
	cellStyle       = tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlack)
)

// DataTable add following behaviors on top of VirtualTable:
// - Add row number column to the left corner
// - Hoist primary key columns to the left right next to row number column
// - Make row number and primary key columns fixed in place
// - Make column names (first row) fixed in place
// - Apply default styling to the fixed columns and rows
// - Allow selecting cell and row (by selecting row number cell)
// - Define default selected style
type DataTable struct {
	*VirtualTable

	// This function is called to get the underlying table cell at any position
	getCells func(row, column int) []*TableCell

	// Rearranged indices of columns. Used to hoist primary key columns to the beginning
	columnIndices []int

	// Number of primary key columns
	pkCount int

	// The currently selected row and column.
	selectedRow, selectedColumn, selectedSubColumn int

	// Status texts for columns
	columnStatuses []*TableCell

	// An optional function which gets called when the user presses Enter on a
	// selected cell. If entire rows selected, the column value is undefined.
	// Likewise for entire columns.
	selected func(row, column int)
}

func NewDataTable() *DataTable {
	vt := NewVirtualTable()
	t := &DataTable{
		VirtualTable:   vt,
		selectedRow:    1,
		selectedColumn: 1,
	}
	vt.SetFixed(1, 1).
		SetGetCellsFunc(t.getStyledCells)
	return t
}

// SetShape sets total number of rows and columns
func (t *DataTable) SetShape(rowCount, columnCount int) *DataTable {
	t.rowCount, t.columnCount = rowCount, columnCount+1
	if t.columnIndices == nil {
		t.SetPrimaryKeyIndices(nil)
	}
	return t
}

// GetShape get number of rows and number of columns
func (t *DataTable) GetShape() (rowCount, columnCount int) {
	return t.rowCount, t.columnCount - 1
}

func (t *DataTable) SetColumnStatuses(cells []*TableCell) *DataTable {
	t.columnStatuses = cells
	t.VirtualTable.SetFixed(2, t.pkCount+1)
	return t
}

// SetSelectedFunc sets a handler which is called whenever the user presses the
// Enter key on a selected cell/row/column. The handler receives the position of
// the selection and its cell contents. If entire rows are selected, the column
// index is undefined. Likewise for entire columns.
func (t *DataTable) SetSelectedFunc(handler func(row, column int)) *DataTable {
	t.selected = handler
	return t
}

// Select sets the selected cell. Depending on the selection settings
// specified via SetSelectable(), this may be an entire row or column, or even
// ignored completely. The "selection changed" event is fired if such a callback
// is available (even if the selection ends up being the same as before and even
// if cells are not selectable).
func (t *DataTable) Select(row, column, subCol int) *DataTable {
	t.selectedRow, t.selectedColumn, t.selectedSubColumn = row, column, subCol
	return t
}

// SetPrimaryKeyIndices records primary key columns and hoist them to the beginning
func (t *DataTable) SetPrimaryKeyIndices(pk []int) *DataTable {
	pkm := map[int]struct{}{}
	for _, i := range pk {
		pkm[i] = struct{}{}
	}
	ordinaryCols := []int{}
	for i := 0; i < t.columnCount; i++ {
		if _, ok := pkm[i]; !ok {
			ordinaryCols = append(ordinaryCols, i)
		}
	}
	t.columnIndices = append(pk, ordinaryCols...)
	t.pkCount = len(pk)
	if t.columnStatuses != nil {
		t.VirtualTable.SetFixed(2, t.pkCount+1)
	} else {
		t.VirtualTable.SetFixed(1, t.pkCount+1)
	}
	return t
}

// SetGetCellsFunc set function to get table cell at a position
func (t *DataTable) SetGetCellsFunc(getCells func(row, column int) []*TableCell) *DataTable {
	t.getCells = getCells
	return t
}

func (t *DataTable) getCellsAt(row, column int) []*TableCell {
	if row < 0 || column < 0 || row >= t.rowCount || column >= t.columnCount {
		return nil
	}
	if t.columnStatuses != nil {
		if row == 0 {
			if column == 0 {
				return []*TableCell{NewTableCell("")}
			}
			statusCell := t.columnStatuses[column-1]
			if statusCell != nil {
				return []*TableCell{statusCell}
			}
			return nil
		}
		row -= 1
	}
	if column == 0 {
		if row == 0 {
			return []*TableCell{NewTableCell("")}
		}
		return []*TableCell{NewTableCell(strconv.Itoa(row - 1))}
	}
	return t.getCells(row, t.columnIndices[column-1])
}

func (t *DataTable) getStyledCells(row, column int) []*TableCell {
	cells := t.getCellsAt(row, column)
	if cells == nil {
		return nil
	}
	if column == 0 {
		cells[0].SetStyle(rowCountStyle)
		return cells
	}
	for i, cell := range cells {
		if row == t.selectedRow && (t.selectedColumn == 0 ||
			(t.selectedColumn == column && (t.selectedSubColumn == i || len(cells) == 1))) {
			cell.FlipStyle().SetTransparency(false)
		} else {
			cell.SetTransparency(true)
		}
	}
	return cells
}

func (t *DataTable) Draw(screen tcell.Screen) {
	t.Box.DrawForSubclass(screen, t)
	_, _, _, height := t.Box.GetInnerRect()
	// Clamp selection
	if t.selectedColumn < 0 {
		t.selectedColumn = 0
	}
	if t.selectedColumn >= t.columnCount {
		t.selectedColumn = t.columnCount - 1
	}
	if t.columnStatuses != nil {
		if t.selectedRow < 2 {
			t.selectedRow = 2
		}
		if t.selectedRow > t.rowCount+1 {
			t.selectedRow = t.rowCount + 1
		}
	} else {
		if t.selectedRow < 1 {
			t.selectedRow = 1
		}
		if t.selectedRow > t.rowCount {
			t.selectedRow = t.rowCount
		}
	}

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

func (t *DataTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return t.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		key := event.Key()
		rowCount, columnCount := t.VirtualTable.GetShape()

		var (
			updateSubCol = func(num int) {
				t.selectedSubColumn = num
				cells := t.getCellsAt(t.selectedRow, t.selectedColumn)
				if len(cells) == 1 {
					t.selectedSubColumn = 0
				}
			}

			home = func() {
				t.selectedRow = 0
				t.selectedColumn = 0
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
				if t.selectedRow < 0 {
					t.selectedRow = 0
				}
				updateSubCol(t.selectedSubColumn)
			}

			left = func() {
				if t.selectedSubColumn > 0 {
					t.selectedSubColumn--
				} else {
					t.selectedColumn--
					if t.selectedColumn < 0 {
						t.selectedColumn = 0
					}
					updateSubCol(1)
				}
			}

			right = func() {
				cells := t.getCellsAt(t.selectedRow, t.selectedColumn)
				if t.selectedSubColumn < len(cells)-1 {
					t.selectedSubColumn++
				} else {
					t.selectedSubColumn = 0
					t.selectedColumn++
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
				if t.selectedRow < 0 {
					t.selectedRow = 0
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
				t.selected(t.selectedRow, t.selectedColumn)
			}
		}
	})
}

// MouseHandler returns the mouse handler for this primitive.
func (t *DataTable) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
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
			if selectEvent {
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
