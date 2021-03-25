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
}

func NewProgressBar(desc string) *ProgressBar {
	b := &ProgressBar{
		TextView: tview.NewTextView(),
		desc:     desc,
	}
	return b
}

func (b *ProgressBar) SetCurrent(num int64) *ProgressBar {
	b.current = num
	return b
}

func (b *ProgressBar) SetTotal(num int64) *ProgressBar {
	b.total = num
	return b
}

func (b *ProgressBar) Draw(screen tcell.Screen) {
	_, _, width, _ := b.GetInnerRect()
	progressStr := fmt.Sprintf("(%d/%d)", b.current, b.total)
	descWidth := stringWidth(b.desc)
	margin := 1
	pStrWidth := stringWidth(progressStr)
	barWidth := width - descWidth - pStrWidth - margin*2
	progressWidth := barWidth * int(b.current) / int(b.total)
	remainingWidth := barWidth - progressWidth
	b.TextView.Clear()
	fmt.Fprintf(b.TextView, "%s [black:white]%s[white]%s %s", b.desc, strings.Repeat(" ", progressWidth), strings.Repeat(" ", remainingWidth), progressStr)
	b.TextView.Draw(screen)
}
