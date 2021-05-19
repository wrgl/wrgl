// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// TableCell represents one cell inside a Table.
type TableCell struct {
	// The text to be displayed in the table cell.
	Text string

	// The alignment of the cell text. One of AlignLeft (default), AlignCenter,
	// or AlignRight.
	Align int

	// The maximum width of the cell in screen space. This is used to give a
	// column a maximum width. Any cell text whose screen width exceeds this width
	// is cut off. Set to 0 if there is no maximum width.
	MaxWidth int

	// If the total table width is less than the available width, this value is
	// used to add extra width to a column. See SetExpansion() for details.
	Expansion int

	// The color of the cell text.
	Color tcell.Color

	// The background color of the cell.
	BackgroundColor tcell.Color

	// If set to true, the BackgroundColor is not used and the cell will have
	// the background color of the table.
	Transparent bool

	// The style attributes of the cell.
	Attributes tcell.AttrMask

	// An optional handler for mouse clicks.
	Clicked func() bool

	// The position and width of the cell the last time table was drawn.
	x, y, width int
}

func NewTableCell(text string) *TableCell {
	return &TableCell{
		Text:            text,
		Align:           tview.AlignLeft,
		Color:           tview.Styles.PrimaryTextColor,
		BackgroundColor: tview.Styles.PrimitiveBackgroundColor,
		Transparent:     true,
	}
}

// SetText sets the cell's text.
func (c *TableCell) SetText(text string) *TableCell {
	c.Text = text
	return c
}

// SetAlign sets the cell's text alignment, one of AlignLeft, AlignCenter, or
// AlignRight.
func (c *TableCell) SetAlign(align int) *TableCell {
	c.Align = align
	return c
}

// SetMaxWidth sets maximum width of the cell in screen space. This is used to
// give a column a maximum width. Any cell text whose screen width exceeds this
// width is cut off. Set to 0 if there is no maximum width.
func (c *TableCell) SetMaxWidth(maxWidth int) *TableCell {
	c.MaxWidth = maxWidth
	return c
}

// SetExpansion sets the value by which the column of this cell expands if the
// available width for the table is more than the table width (prior to applying
// this expansion value). This is a proportional value. The amount of unused
// horizontal space is divided into widths to be added to each column. How much
// extra width a column receives depends on the expansion value: A value of 0
// (the default) will not cause the column to increase in width. Other values
// are proportional, e.g. a value of 2 will cause a column to grow by twice
// the amount of a column with a value of 1.
//
// Since this value affects an entire column, the maximum over all visible cells
// in that column is used.
//
// This function panics if a negative value is provided.
func (c *TableCell) SetExpansion(expansion int) *TableCell {
	if expansion < 0 {
		panic("Table cell expansion values may not be negative")
	}
	c.Expansion = expansion
	return c
}

// SetTextColor sets the cell's text color.
func (c *TableCell) SetTextColor(color tcell.Color) *TableCell {
	c.Color = color
	return c
}

// SetBackgroundColor sets the cell's background color. This will also cause the
// cell's Transparent flag to be set to "false".
func (c *TableCell) SetBackgroundColor(color tcell.Color) *TableCell {
	c.BackgroundColor = color
	c.Transparent = false
	return c
}

// SetTransparency sets the background transparency of this cell. A value of
// "true" will cause the cell to use the table's background color. A value of
// "false" will cause it to use its own background color.
func (c *TableCell) SetTransparency(transparent bool) *TableCell {
	c.Transparent = transparent
	return c
}

// SetAttributes sets the cell's text attributes. You can combine different
// attributes using bitmask operations:
//
//   cell.SetAttributes(tcell.AttrUnderline | tcell.AttrBold)
func (c *TableCell) SetAttributes(attr tcell.AttrMask) *TableCell {
	c.Attributes = attr
	return c
}

// SetStyle sets the cell's style (foreground color, background color, and
// attributes) all at once.
func (c *TableCell) SetStyle(style tcell.Style) *TableCell {
	c.Color, c.BackgroundColor, c.Attributes = style.Decompose()
	return c
}

func (c *TableCell) FlipStyle() *TableCell {
	c.Color, c.BackgroundColor = c.BackgroundColor, c.Color
	return c
}

// GetPosition returns the position of the table cell on screen.
// If the cell is not on screen, the return values are
func (c *TableCell) GetPosition() (x, y, width int) {
	return c.x, c.y, c.width
}

// SetPosition sets position on screen for table cell.
func (c *TableCell) SetPosition(x, y, width int) *TableCell {
	c.x, c.y, c.width = x, y, width
	return c
}

// SetClickedFunc sets a handler which fires when this cell is clicked.
func (c *TableCell) SetClickedFunc(clicked func() bool) *TableCell {
	c.Clicked = clicked
	return c
}
