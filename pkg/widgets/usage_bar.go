package widgets

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

var (
	errCombinedColumnWidths = fmt.Errorf("combined column widths greater than total width")
)

type UsageBar struct {
	*tview.TextView
	strs           []string
	widths         []int
	margin         int
	colWidths      []int
	lastTotalWidth int
	height         int
}

func NewUsageBar(entries [][2]string, margin int) *UsageBar {
	n := len(entries)
	u := &UsageBar{
		TextView: tview.NewTextView().
			SetDynamicColors(true),
		strs:      make([]string, n),
		widths:    make([]int, n),
		margin:    margin,
		colWidths: []int{},
	}
	for i, a := range entries {
		u.strs[i] = fmt.Sprintf("[black:white] %s [white:black] %s", a[0], a[1])
		u.widths[i] = len(a[0]) + stringWidth(a[1]) + 3
	}
	return u
}

func (b *UsageBar) setMaxColWidth(col, width, totalWidth int) error {
	if len(b.colWidths) <= col {
		b.colWidths = append(b.colWidths, width)
	}
	if b.colWidths[col] < width {
		b.colWidths[col] = width
		sum := b.colWidths[0]
		for _, n := range b.colWidths[1:] {
			sum += n + b.margin
		}
		if sum > totalWidth {
			return errCombinedColumnWidths
		}
	}
	return nil
}

func (b *UsageBar) computeColumnWidths(totalWidth, maxColumn int) error {
	rem := totalWidth
	col := 0
	b.colWidths = b.colWidths[:0]
	for _, w := range b.widths {
		if (col > maxColumn) || (rem > 0 && w+b.margin > rem) || (rem == 0 && col >= len(b.colWidths)) {
			col = 0
			rem = 0
		}
		err := b.setMaxColWidth(col, w, totalWidth)
		if err != nil {
			return err
		}
		if rem > 0 {
			rem -= b.colWidths[col] + b.margin
		}
		col++
	}
	return nil
}

func (b *UsageBar) printRows(totalWidth int) {
	maxColumn := len(b.widths) - 1
	for {
		err := b.computeColumnWidths(totalWidth, maxColumn)
		if err == errCombinedColumnWidths {
			if len(b.colWidths) == 1 {
				break
			}
			maxColumn = len(b.colWidths) - 2
			continue
		}
		if err != nil {
			panic(err)
		}
		break
	}
	b.TextView.Clear()
	col := 0
	row := []string{}
	b.height = 0
	for i, s := range b.strs {
		if col >= len(b.colWidths) {
			fmt.Fprintln(b.TextView, strings.Join(
				row, strings.Repeat(" ", b.margin),
			))
			fmt.Fprintln(b.TextView, "")
			row = row[:0]
			col = 0
			b.height += 2
		}
		spaces := b.colWidths[col] - b.widths[i]
		if spaces < 0 {
			spaces = 0
		}
		row = append(row, fmt.Sprintf("%s%s", s, strings.Repeat(" ", spaces)))
		col++
	}
	fmt.Fprint(b.TextView, strings.Join(
		row, strings.Repeat(" ", b.margin),
	))
	b.height++
}

func (b *UsageBar) BeforeDraw(screen tcell.Screen, flex *tview.Flex) {
	_, _, width, _ := b.GetInnerRect()
	if width != b.lastTotalWidth {
		b.printRows(width)
		b.lastTotalWidth = width
	}
	flex.ResizeItem(b, b.height, 1)
}
