package widgets

import (
	"fmt"

	"github.com/rivo/tview"
)

type ColumnsList struct {
	*tview.TextView
	unchanged, added, removed []string
}

func NewColumnsList(unchanged, added, removed []string) *ColumnsList {
	l := &ColumnsList{
		TextView:  tview.NewTextView(),
		unchanged: unchanged,
		added:     added,
		removed:   removed,
	}
	l.TextView.SetBorder(true)
	for _, s := range l.unchanged {
		fmt.Fprintf(l.TextView, "%s\n", s)
	}
	fmt.Fprint(l.TextView, "[green]")
	for _, s := range l.added {
		fmt.Fprintf(l.TextView, "%s\n", s)
	}
	fmt.Fprint(l.TextView, "[red]")
	for _, s := range l.removed {
		fmt.Fprintf(l.TextView, "%s\n", s)
	}
	return l
}
