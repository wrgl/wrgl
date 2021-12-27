// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"sort"

	"github.com/gdamore/tcell/v2"
	"github.com/lucasb-eyer/go-colorful"
	"github.com/rivo/tview"
)

type VirtualTable struct {
	*tview.Box

	// Whether or not this table has borders around each cell.
	borders bool

	// The color of the borders or the separator.
	bordersColor tcell.Color

	// If there are no borders, the column separator.
	separator    rune
	subSeparator rune

	// The number of fixed rows / columns.
	fixedRows, fixedColumns int

	// The total number of rows / columns.
	rowCount, columnCount int

	// The number of rows/columns by which the table is scrolled down/to the
	// right.
	rowOffset, columnOffset int

	// The number of visible rows the last time the table was drawn.
	visibleRows int

	// The indices of the visible columns as of the last time the table was drawn.
	visibleColumnIndices []int

	// The net widths of the visible columns as of the last time the table was
	// drawn.
	visibleColumns []*tableColumn

	// An optional function which gets called when the user presses Escape, Tab,
	// or Backtab.
	done func(key tcell.Key)

	// This function is called to get the underlying table cell at any position
	getCells func(row, column int) []*TableCell

	// This function is called when visible row and column indices change
	onVisibleCellsChange func(rows, columns []int)

	// If this is not -1, always try to keep this column in view
	keptInViewColumn int
}

func NewVirtualTable() *VirtualTable {
	return &VirtualTable{
		Box:              tview.NewBox(),
		bordersColor:     tview.Styles.GraphicsColor,
		keptInViewColumn: -1,
		separator:        ' ',
		subSeparator:     ' ',
	}
}

// SetFixed sets the number of fixed rows and columns which are always visible
// even when the rest of the cells are scrolled out of view. Rows are always the
// top-most ones. Columns are always the left-most ones.
func (t *VirtualTable) SetFixed(rows, columns int) *VirtualTable {
	t.fixedRows, t.fixedColumns = rows, columns
	return t
}

// SetDoneFunc sets a handler which is called whenever the user presses the
// Escape, Tab, or Backtab key.
func (t *VirtualTable) SetDoneFunc(handler func(key tcell.Key)) *VirtualTable {
	t.done = handler
	return t
}

// SetBorders sets whether or not each cell in the table is surrounded by a
// border.
func (t *VirtualTable) SetBorders(show bool) *VirtualTable {
	t.borders = show
	return t
}

// SetBordersColor sets the color of the cell borders.
func (t *VirtualTable) SetBordersColor(color tcell.Color) *VirtualTable {
	t.bordersColor = color
	return t
}

func (t *VirtualTable) SetOnVisibleCellsChangeFunc(handler func(rows, columns []int)) *VirtualTable {
	t.onVisibleCellsChange = handler
	return t
}

func (t *VirtualTable) KeepColumnInView(column int) *VirtualTable {
	t.keptInViewColumn = column
	return t
}

// SetSeparator sets the character used to fill the space between two
// neighboring cells. This is a space character ' ' per default but you may
// want to set it to Borders.Vertical (or any other rune) if the column
// separation should be more visible. If cell borders are activated, this is
// ignored.
//
// Separators have the same color as borders.
func (t *VirtualTable) SetSeparator(separator rune) *VirtualTable {
	t.separator = separator
	return t
}

// SetSubSeparator sets the character used to fill the space between two
// sub cells. This is a space character ' ' per default but you may
// want to set it to Borders.Vertical (or any other rune) if the column
// separation should be more visible. If cell borders are activated, this is
// ignored.
//
// Separators have the same color as borders.
func (t *VirtualTable) SetSubSeparator(separator rune) *VirtualTable {
	t.subSeparator = separator
	return t
}

// SetOffset sets how many rows and columns should be skipped when drawing the
// table. This is useful for large tables that do not fit on the screen.
//
// Fixed rows and columns are never skipped.
func (t *VirtualTable) SetOffset(row, column int) *VirtualTable {
	t.rowOffset, t.columnOffset = row, column
	return t
}

// GetOffset returns the current row and column offset. This indicates how many
// rows and columns the table is scrolled down and to the right.
func (t *VirtualTable) GetOffset() (row, column int) {
	return t.rowOffset, t.columnOffset
}

// SetShape sets total number of rows and columns
func (t *VirtualTable) SetShape(rowCount, columnCount int) *VirtualTable {
	t.rowCount, t.columnCount = rowCount, columnCount
	return t
}

// GetShape get number of rows and number of columns
func (t *VirtualTable) GetShape() (rowCount, columnCount int) {
	return t.rowCount, t.columnCount
}

// ScrollToBeginning scrolls the table to the beginning to that the top left
// corner of the table is shown.
func (t *VirtualTable) ScrollToBeginning() *VirtualTable {
	t.columnOffset = 0
	t.rowOffset = 0
	return t
}

// ScrollToEnd scrolls the table to the beginning to that the bottom left corner
// of the table is shown. Adding more rows to the table will cause it to
// automatically scroll with the new data.
func (t *VirtualTable) ScrollToEnd() *VirtualTable {
	t.columnOffset = 0
	t.rowOffset = t.rowCount
	return t
}

// SetGetCellsFunc set function to get table cell at a position
func (t *VirtualTable) SetGetCellsFunc(getCell func(row, column int) []*TableCell) *VirtualTable {
	t.getCells = getCell
	return t
}

// cellAt returns the row and column located at the given screen coordinates.
// Each returned value may be negative if there is no row and/or cell. This
// function will also process coordinates outside the table's inner rectangle so
// callers will need to check for bounds themselves.
func (t *VirtualTable) cellAt(x, y int) (row, column, subColumn int) {
	rectX, rectY, _, _ := t.GetInnerRect()

	// Determine row as seen on screen.
	if t.borders {
		row = (y - rectY - 1) / 2
	} else {
		row = y - rectY
	}

	// Respect fixed rows and row offset.
	if row >= 0 {
		if row >= t.fixedRows {
			row += t.rowOffset
		}
		if row >= t.rowCount {
			row = -1
		}
	}

	// Search for the clicked column.
	column = -1
	if x >= rectX {
		columnX := rectX
		if t.borders {
			columnX++
		}
	columnsLoop:
		for index, col := range t.visibleColumns {
			offset := columnX
			for i, width := range col.Widths(row) {
				offset += width + 1
				if x < offset {
					column = t.visibleColumnIndices[index]
					subColumn = i
					break columnsLoop
				}
			}
			columnX += col.Width + 1
		}
	}

	return
}

func (t *VirtualTable) clampOffsets() {
	if t.rowCount-t.rowOffset < t.visibleRows {
		t.rowOffset = t.rowCount - t.visibleRows
	}
	if t.rowOffset < 0 {
		t.rowOffset = 0
	}
	if t.columnOffset < 0 {
		t.columnOffset = 0
	}
}

// Determine the indices and widths of the columns and rows which fit on the screen.
func (t *VirtualTable) determineIndicesAndWidths(width int) (rows []int) {
	var (
		columns                 []*tableColumn
		columnIndices           []int
		tableHeight, tableWidth int
	)
	if t.borders {
		tableWidth = 1 // We start at the second character because of the left table border.
	}
	indexRow := func(row int) bool { // Determine if this row is visible, store its index.
		if tableHeight >= t.visibleRows {
			return false
		}
		rows = append(rows, row)
		tableHeight++
		return true
	}
	for row := 0; row < t.fixedRows && row < t.rowCount; row++ { // Do the fixed rows first.
		if !indexRow(row) {
			break
		}
	}
	for row := t.fixedRows + t.rowOffset; row < t.rowCount; row++ { // Then the remaining rows.
		if !indexRow(row) {
			break
		}
	}
	var (
		skipped, expansionTotal int
		expansions              []int
	)
ColumnLoop:
	for columnIndex := 0; ; columnIndex++ {
		// If we've moved beyond the right border, we stop or skip a column.
		for tableWidth-1 >= width { // -1 because we include one extra column if the separator falls on the right end of the box.
			// We've moved beyond the available space.
			if columnIndex < t.fixedColumns {
				break ColumnLoop // We're in the fixed area. We're done.
			}
			if t.keptInViewColumn != -1 && t.keptInViewColumn-skipped == t.fixedColumns {
				break ColumnLoop // The selected column reached the leftmost point before disappearing.
			}
			if t.keptInViewColumn != -1 && skipped >= t.columnOffset && t.keptInViewColumn < columnIndex-1 {
				break ColumnLoop // We've skipped as many as requested and the selection is visible.
			}
			if t.keptInViewColumn == -1 && skipped >= t.columnOffset {
				break ColumnLoop // There is no selection and we've already reached the offset.
			}
			if len(columns) <= t.fixedColumns {
				break // Nothing to skip.
			}

			// We need to skip a column.
			skipped++
			tableWidth -= columns[t.fixedColumns].Width + 1
			columnIndices = append(columnIndices[:t.fixedColumns], columnIndices[t.fixedColumns+1:]...)
			columns = append(columns[:t.fixedColumns], columns[t.fixedColumns+1:]...)
			expansions = append(expansions[:t.fixedColumns], expansions[t.fixedColumns+1:]...)
		}

		// What is this column's width (without expansion)?
		column := newTableColumn()
		for i, rowIndex := range rows {
			cells := t.getCells(rowIndex, columnIndex)
			cellWidths := []int{}
			cellExpansions := []int{}
			for _, cell := range cells {
				_, _, _, _, _, _, cellWidth := decomposeString(cell.Text, true, false)
				if cell.MaxWidth > 0 && cell.MaxWidth < cellWidth {
					cellWidth = cell.MaxWidth
				}
				cellWidths = append(cellWidths, cellWidth)
				cellExpansions = append(cellExpansions, cell.Expansion)
			}
			column.UpdateWidths(i, cellWidths)
			column.UpdateExpansions(i, cellExpansions)
		}
		column.DistributeWidth()
		if column.Width < 0 {
			break // No more cells found in this column.
		}

		// Store new column info at the end.
		columnIndices = append(columnIndices, columnIndex)
		columns = append(columns, column)
		tableWidth += column.Width + 1
		expansions = append(expansions, 1)
		expansionTotal += 1
	}
	t.columnOffset = skipped

	// If we have space left, distribute it.
	if tableWidth < width {
		toDistribute := width - tableWidth
		for index, expansion := range expansions {
			if expansionTotal <= 0 {
				break
			}
			expWidth := toDistribute * expansion / expansionTotal
			columns[index].DistributeExpansionWidth(expWidth)
			toDistribute -= expWidth
			expansionTotal -= expansion
		}
	}
	t.visibleColumnIndices, t.visibleColumns = columnIndices, columns
	return
}

func (t *VirtualTable) drawBorder(screen tcell.Screen, x, y, colX, rowY int, ch rune) {
	borderStyle := tcell.StyleDefault.Background(t.GetBackgroundColor()).Foreground(t.bordersColor)
	screen.SetContent(x+colX, y+rowY, ch, nil, borderStyle)
}

func (t *VirtualTable) drawCells(screen tcell.Screen, rows []int, x, y, width, height, totalHeight int) (columnX int) {
	// Draw the cells (and borders).
	if !t.borders {
		columnX--
	}
	for colIndInd, colIndex := range t.visibleColumnIndices {
		column := t.visibleColumns[colIndInd]
	rowLoop:
		for rowY, row := range rows {
			// Get the cell.
			cells := t.getCells(row, colIndex)
			widths := column.Widths(rowY)

			subColumnX := 0
			for i, colWidth := range widths {
				if t.borders {
					// Draw borders.
					cellY := rowY * 2
					for pos := 0; pos < colWidth && columnX+subColumnX+1+pos < width; pos++ {
						t.drawBorder(screen, x, y, columnX+subColumnX+pos+1, cellY, tview.Borders.Horizontal)
					}
					ch := tview.Borders.Cross
					if colIndInd == 0 {
						if cellY == 0 {
							ch = tview.Borders.TopLeft
						} else {
							ch = tview.Borders.LeftT
						}
					} else if cellY == 0 {
						ch = tview.Borders.TopT
					}
					t.drawBorder(screen, x, y, columnX+subColumnX, cellY, ch)
					cellY++
					if cellY >= height || y+cellY >= totalHeight {
						break // No space for the text anymore.
					}
					t.drawBorder(screen, x, y, columnX+subColumnX, cellY, tview.Borders.Vertical)
				} else if colIndInd > 0 {
					// Draw separator.
					if subColumnX > 0 {
						t.drawBorder(screen, x, y, columnX+subColumnX, rowY, t.subSeparator)
					} else {
						t.drawBorder(screen, x, y, columnX+subColumnX, rowY, t.separator)
					}
				}

				if colWidth == -1 {
					continue rowLoop
				}

				textY := y + rowY
				if t.borders {
					textY = y + rowY*2 + 1
				}

				// Draw text for the first sub-cell
				finalWidth := colWidth
				if columnX+subColumnX+1+colWidth >= width {
					finalWidth = width - columnX - subColumnX - 1
				}
				cell := cells[i]
				// cell.SetPosition(x+columnX+subColumnX+1, textY, finalWidth)
				_, printed, _, _ := printWithStyle(
					screen,
					cell.Text,
					x+columnX+subColumnX+1,
					textY,
					0,
					finalWidth,
					cell.Align,
					tcell.StyleDefault.Foreground(cell.Color()).Attributes(cell.Attributes),
					true,
				)
				if tview.TaggedStringWidth(cell.Text)-printed > 0 && printed > 0 {
					_, _, style, _ := screen.GetContent(x+columnX+subColumnX+finalWidth, textY)
					printWithStyle(
						screen,
						string(tview.SemigraphicsHorizontalEllipsis),
						x+columnX+subColumnX+finalWidth,
						0,
						textY,
						1,
						tview.AlignLeft,
						style,
						false,
					)
				}
				subColumnX += colWidth + 1
			}
		}

		// Draw bottom border.
		if rowY := 2 * len(rows); t.borders && rowY < height {
			for pos := 0; pos < column.Width && columnX+1+pos < width; pos++ {
				t.drawBorder(screen, x, y, columnX+pos+1, rowY, tview.Borders.Horizontal)
			}
			ch := tview.Borders.BottomT
			if colIndInd == 0 {
				ch = tview.Borders.BottomLeft
			}
			t.drawBorder(screen, x, y, columnX, rowY, ch)
		}

		columnX += column.Width + 1
	}
	return
}

// Draw right border.
func (t *VirtualTable) drawRightBorder(screen tcell.Screen, rows []int, columnX, x, y, width, height int) {
	if t.borders && t.rowCount > 0 && columnX < width {
		for rowY := range rows {
			rowY *= 2
			if rowY+1 < height {
				t.drawBorder(screen, x, y, columnX, rowY+1, tview.Borders.Vertical)
			}
			ch := tview.Borders.RightT
			if rowY == 0 {
				ch = tview.Borders.TopRight
			}
			t.drawBorder(screen, x, y, columnX, rowY, ch)
		}
		if rowY := 2 * len(rows); rowY < height {
			t.drawBorder(screen, x, y, columnX, rowY, tview.Borders.BottomRight)
		}
	}
}

// Helper function which colors the background of a box.
// backgroundTransparent == true => Don't modify background color (when invert == false).
// textTransparent == true => Don't modify text color (when invert == false).
// attr == 0 => Don't change attributes.
// invert == true => Ignore attr, set text to backgroundColor or t.backgroundColor;
//                   set background to textColor.
func (t *VirtualTable) colorBackground(screen tcell.Screen, x, y, width, height, fromX, fromY, w, h int, backgroundColor, textColor tcell.Color, backgroundTransparent, textTransparent bool, attr tcell.AttrMask, invert bool) {
	for by := 0; by < h && fromY+by < y+height; by++ {
		for bx := 0; bx < w && fromX+bx < x+width; bx++ {
			m, c, style, _ := screen.GetContent(fromX+bx, fromY+by)
			fg, bg, a := style.Decompose()
			if invert {
				style = style.Background(textColor).Foreground(backgroundColor)
			} else {
				if !backgroundTransparent {
					bg = backgroundColor
				}
				if !textTransparent {
					fg = textColor
				}
				if attr != 0 {
					a = attr
				}
				style = style.Background(bg).Foreground(fg).Attributes(a)
			}
			screen.SetContent(fromX+bx, fromY+by, m, c, style)
		}
	}
}

// colorCellBackgrounds colors the cell backgrounds. To avoid undesirable artefacts, we combine
// the drawing of a cell by background color, selected cells last.
func (t *VirtualTable) colorCellBackgrounds(screen tcell.Screen, x, y, width, height int, rows []int) {
	type cellInfo struct {
		x, y, w, h int
		cell       *TableCell
	}
	cellsByBackgroundColor := make(map[tcell.Color][]*cellInfo)
	var backgroundColors []tcell.Color
	for rowY, row := range rows {
		columnX := 0
		for colIndInd, colIndex := range t.visibleColumnIndices {
			column := t.visibleColumns[colIndInd]
			cells := t.getCells(row, colIndex)
			widths := column.Widths(rowY)
			subColumnX := 0
			for i, cell := range cells {
				bx, by, bw, bh := x+columnX+subColumnX, y+rowY, widths[i]+1, 1
				if t.borders {
					by = y + rowY*2
					bw++
					bh = 3
				}
				entries, ok := cellsByBackgroundColor[cell.BackgroundColor()]
				cellsByBackgroundColor[cell.BackgroundColor()] = append(entries, &cellInfo{
					x:    bx,
					y:    by,
					w:    bw,
					h:    bh,
					cell: cell,
				})
				if !ok {
					backgroundColors = append(backgroundColors, cell.BackgroundColor())
				}
				subColumnX += widths[i] + 1
			}
			columnX += column.Width + 1
		}
	}
	sort.Slice(backgroundColors, func(i int, j int) bool {
		// Draw brightest colors last (i.e. on top).
		r, g, b := backgroundColors[i].RGB()
		c := colorful.Color{R: float64(r) / 255, G: float64(g) / 255, B: float64(b) / 255}
		_, _, li := c.Hcl()
		r, g, b = backgroundColors[j].RGB()
		c = colorful.Color{R: float64(r) / 255, G: float64(g) / 255, B: float64(b) / 255}
		_, _, lj := c.Hcl()
		return li < lj
	})
	for _, bgColor := range backgroundColors {
		entries := cellsByBackgroundColor[bgColor]
		for _, info := range entries {
			t.colorBackground(screen, x, y, width, height, info.x, info.y, info.w, info.h, bgColor, info.cell.Color(), info.cell.Transparent, true, 0, false)
		}
	}
}

// Draw draws this primitive onto the screen.
func (t *VirtualTable) Draw(screen tcell.Screen) {
	t.Box.DrawForSubclass(screen, t)
	_, totalHeight := screen.Size()
	x, y, width, height := t.GetInnerRect()
	if t.borders {
		t.visibleRows = height / 2
	} else {
		t.visibleRows = height
	}
	t.clampOffsets()
	rows := t.determineIndicesAndWidths(width)
	columnX := t.drawCells(screen, rows, x, y, width, height, totalHeight)
	t.drawRightBorder(screen, rows, columnX, x, y, width, height)
	t.colorCellBackgrounds(screen, x, y, width, height, rows)
	if t.onVisibleCellsChange != nil {
		t.onVisibleCellsChange(rows, t.visibleColumnIndices)
	}
}

// InputHandler returns the handler for this primitive.
func (t *VirtualTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return t.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		key := event.Key()

		if key == tcell.KeyEscape || key == tcell.KeyTab || key == tcell.KeyBacktab {
			if t.done != nil {
				t.done(key)
			}
			return
		}

		// Movement functions.
		var (
			home = func() {
				t.rowOffset = 0
				t.columnOffset = 0
			}

			end = func() {
				t.columnOffset = 0
			}

			down = func() {
				t.rowOffset++
			}

			up = func() {
				t.rowOffset--
			}

			left = func() {
				t.columnOffset--
			}

			right = func() {
				t.columnOffset++
			}

			pageDown = func() {
				offsetAmount := t.visibleRows - t.fixedRows
				if offsetAmount < 0 {
					offsetAmount = 0
				}

				t.rowOffset += offsetAmount
			}

			pageUp = func() {
				offsetAmount := t.visibleRows - t.fixedRows
				if offsetAmount < 0 {
					offsetAmount = 0
				}

				t.rowOffset -= offsetAmount
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
		}
	})
}

// MouseHandler returns the mouse handler for this primitive.
func (t *VirtualTable) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
	return t.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
		x, y := event.Position()
		if !t.InRect(x, y) {
			return false, nil
		}

		switch action {
		case tview.MouseLeftClick:
			row, column, subCol := t.cellAt(x, y)
			if row >= 0 && row < t.rowCount && column >= 0 && column < t.columnCount {
				cells := t.getCells(row, column)
				var cell *TableCell
				if len(cells) > 1 {
					cell = cells[subCol]
				} else if len(cells) == 1 {
					cell = cells[0]
				}
				if cell != nil && cell.Clicked != nil {
					cell.Clicked()
				}
			}
			setFocus(t)
			consumed = true
		case tview.MouseScrollUp:
			t.rowOffset--
			consumed = true
		case tview.MouseScrollDown:
			t.rowOffset++
			consumed = true
		}

		return
	})
}
