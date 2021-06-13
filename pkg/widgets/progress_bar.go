// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ProgressBar struct {
	*tview.TextView
	desc    string
	total   int64
	current int64
	updated bool
}

func NewProgressBar(desc string) *ProgressBar {
	b := &ProgressBar{
		TextView: tview.NewTextView().
			SetDynamicColors(true),
		desc: desc,
	}
	b.TextView.SetTextColor(tcell.ColorWhite).
		SetBackgroundColor(tcell.ColorBlack)
	return b
}

func (b *ProgressBar) SetCurrent(num int64) *ProgressBar {
	if num != b.current {
		b.updated = true
		b.current = num
	}
	return b
}

func (b *ProgressBar) SetTotal(num int64) *ProgressBar {
	if num != b.total {
		b.updated = true
		b.total = num
	}
	return b
}

func (b *ProgressBar) printText() {
	if b.total == 0 {
		return
	}
	_, _, width, _ := b.GetInnerRect()
	mainStr := fmt.Sprintf("%s (%d/%d)", b.desc, b.current, b.total)
	mainWidth := stringWidth(mainStr)
	spaceWidth := (width - mainWidth) / 2
	if spaceWidth < 0 {
		spaceWidth = 0
	}
	txt := fmt.Sprintf("%s%s%s", strings.Repeat(" ", spaceWidth), mainStr, strings.Repeat(" ", width-mainWidth-spaceWidth))
	barWidth := int((int64(width) * b.current) / b.total)
	if barWidth == width {
		txt = fmt.Sprintf("[black:white]%s", txt)
	} else if barWidth > 0 {
		txt = fmt.Sprintf("[black:white]%s[white:black]%s", txt[:barWidth], txt[barWidth:])
	}
	b.TextView.Clear()
	fmt.Fprint(b.TextView, txt)
	b.updated = false
}

func (b *ProgressBar) Draw(screen tcell.Screen) {
	b.Box.DrawForSubclass(screen, b)
	if b.updated {
		b.printText()
	}
	b.TextView.Draw(screen)
}
