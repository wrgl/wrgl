// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"fmt"

	"github.com/rivo/tview"
)

func CreateColumnsList(unchanged, added, removed []string) *tview.TextView {
	tv := tview.NewTextView().SetDynamicColors(true)
	tv.SetBorder(true)
	for _, s := range unchanged {
		fmt.Fprintf(tv, "%s\n", s)
	}
	fmt.Fprint(tv, "[green]")
	for _, s := range added {
		fmt.Fprintf(tv, "%s\n", s)
	}
	fmt.Fprint(tv, "[red]")
	for _, s := range removed {
		fmt.Fprintf(tv, "%s\n", s)
	}
	return tv
}
