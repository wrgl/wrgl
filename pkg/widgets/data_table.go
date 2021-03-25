package widgets

import (
	"strconv"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

var (
	columnStyle       = tcell.StyleDefault.Foreground(tcell.ColorAzure).Bold(true)
	rowCountStyle     = tcell.StyleDefault.Foreground(tcell.ColorSlateGray)
	primaryKeyStyle   = tcell.StyleDefault.Foreground(tcell.ColorAquaMarine)
	cellStyle         = tcell.StyleDefault
	selectedPKStyle   tcell.Style
	selectedCellStyle tcell.Style
)

func init() {
	fg, _, attr := primaryKeyStyle.Decompose()
	selectedPKStyle = tcell.StyleDefault.Foreground(tcell.ColorBlack).Background(fg).Attributes(attr)
	_, _, attr = cellStyle.Decompose()
	selectedCellStyle = tcell.StyleDefault.Foreground(tcell.ColorBlack).Background(tcell.ColorWhite).Attributes(attr)
}

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
	getCell func(row, column int) *TableCell

	// Rearranged indices of columns. Used to hoist primary key columns to the beginning
	columnIndices []int

	// Number of primary key columns
	pkCount int

	// The currently selected row and column.
	selectedRow, selectedColumn int

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
		SetGetCellFunc(t.modifiedGetCell)
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
func (t *DataTable) Select(row, column int) *DataTable {
	t.selectedRow, t.selectedColumn = row, column
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
	t.VirtualTable.SetFixed(1, t.pkCount+1)
	return t
}

// SetGetCellFunc set function to get table cell at a position
func (t *DataTable) SetGetCellFunc(getCell func(row, column int) *TableCell) *DataTable {
	t.getCell = getCell
	return t
}

func (t *DataTable) modifiedGetCell(row, column int) *TableCell {
	if row < 0 || column < 0 || row >= t.rowCount || column >= t.columnCount {
		return nil
	}
	if column == 0 {
		if row == 0 {
			return NewTableCell("")
		}
		return NewTableCell(strconv.Itoa(row - 1)).SetStyle(rowCountStyle)
	}
	if row == 0 {
		return t.getCell(row, t.columnIndices[column-1]).SetStyle(columnStyle)
	}
	if column <= t.pkCount {
		cell := t.getCell(row, t.columnIndices[column-1])
		if row == t.selectedRow && (t.selectedColumn == 0 || t.selectedColumn == column) {
			return cell.SetStyle(selectedPKStyle).SetTransparency(false)
		}
		return cell.SetStyle(primaryKeyStyle).SetTransparency(true)
	}
	cell := t.getCell(row, t.columnIndices[column-1])
	if row == t.selectedRow && (t.selectedColumn == 0 || t.selectedColumn == column) {
		return cell.SetStyle(selectedCellStyle).SetTransparency(false)
	}
	return cell.SetStyle(cellStyle).SetTransparency(true)
}

func (t *DataTable) Draw(screen tcell.Screen) {
	t.Box.DrawForSubclass(screen, t)
	_, _, _, height := t.Box.GetInnerRect()
	// Clamp selection
	if t.selectedColumn < 0 {
		t.selectedColumn = 0
	}
	if t.selectedRow < 1 {
		t.selectedRow = 1
	}
	if t.selectedColumn >= t.columnCount {
		t.selectedColumn = t.columnCount - 1
	}
	if t.selectedRow >= t.rowCount {
		t.selectedRow = t.rowCount - 1
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
			home = func() {
				t.selectedRow = 0
				t.selectedColumn = 0
			}

			end = func() {
				t.selectedRow = rowCount - 1
				t.selectedColumn = columnCount - 1
			}

			down = func() {
				t.selectedRow++
				if t.selectedRow >= rowCount {
					t.selectedRow = rowCount - 1
				}
			}

			up = func() {
				t.selectedRow--
				if t.selectedRow < 0 {
					t.selectedRow = 0
				}
			}

			left = func() {
				t.selectedColumn--
				if t.selectedColumn < 0 {
					t.selectedColumn = 0
				}
			}

			right = func() {
				t.selectedColumn++
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
			row, column := t.cellAt(x, y)
			if row >= 0 && row < rowCount && column >= 0 && column < columnCount {
				cell := t.modifiedGetCell(row, column)
				if cell != nil && cell.Clicked != nil {
					if noSelect := cell.Clicked(); noSelect {
						selectEvent = false
					}
				}
			}
			if selectEvent {
				t.Select(row, column)
			}
			setFocus(t)
			consumed = true
		default:
			return t.VirtualTable.MouseHandler()(action, event, setFocus)
		}

		return
	})
}
