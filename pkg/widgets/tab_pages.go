package widgets

import (
	"bytes"
	"fmt"

	"github.com/rivo/tview"
)

type TabPages struct {
	*tview.Flex
	pages          *tview.Pages
	tabBar, margin *tview.TextView
	labels         []string
	items          []tview.Primitive
}

func NewTabPages() *TabPages {
	p := &TabPages{
		pages:  tview.NewPages(),
		Flex:   tview.NewFlex(),
		tabBar: tview.NewTextView(),
		margin: tview.NewTextView(),
	}
	p.tabBar.SetDynamicColors(true).
		SetRegions(true).
		SetWrap(false).
		SetHighlightedFunc(func(added, removed, remaining []string) {
			p.pages.SwitchToPage(added[0])
		})
	p.Flex.AddItem(p.margin, 1, 1, false).
		AddItem(p.tabBar, 1, 1, false).
		AddItem(p.margin, 1, 1, true).
		AddItem(p.pages, 0, 1, false)
	return p
}

func (p *TabPages) AddTab(label string, item tview.Primitive) *TabPages {
	name := fmt.Sprintf("%d", len(p.labels))
	p.pages.AddPage(name, item, true, true)
	p.items = append(p.items, item)
	p.labels = append(p.labels, label)
	p.writeTabBar()
	return p
}

func (p *TabPages) SetLabel(item tview.Primitive, label string) error {
	for ind, pr := range p.items {
		if pr == item {
			p.labels[ind] = label
			p.writeTabBar()
			return nil
		}
	}
	return fmt.Errorf("TabPages.SetLabel: primitive %v not found", item)
}

func (p *TabPages) writeTabBar() {
	buf := bytes.NewBufferString("")
	for ind, label := range p.labels {
		fmt.Fprintf(buf, `["%d"][darkcyan]%s[white][""]  `, ind, label)
	}
	p.tabBar.SetText(buf.String())
}
