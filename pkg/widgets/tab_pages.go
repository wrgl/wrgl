package widgets

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type TabPages struct {
	*tview.Flex
	mu             sync.Mutex
	app            *tview.Application
	pages          *tview.Pages
	tabBar, margin *tview.TextView
	labels         []string
	items          []tview.Primitive
}

var (
	tabKeys = []string{"q", "w", "e", "r"}
)

func NewTabPages(app *tview.Application) *TabPages {
	p := &TabPages{
		pages:  tview.NewPages(),
		Flex:   tview.NewFlex(),
		app:    app,
		tabBar: tview.NewTextView(),
		margin: tview.NewTextView(),
	}
	p.tabBar.SetDynamicColors(true).
		SetRegions(true).
		SetWrap(false).
		SetHighlightedFunc(func(added, removed, remaining []string) {
			p.pages.SwitchToPage(added[0])
			i, err := strconv.Atoi(added[0])
			if err != nil {
				panic(err)
			}
			p.app.SetFocus(p.items[i])
		})
	p.Flex.SetDirection(tview.FlexRow).
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
	p.tabBar.Highlight(strconv.Itoa(len(p.labels) - 1))
	return p
}

func (p *TabPages) LastTab() tview.Primitive {
	if len(p.items) > 0 {
		return p.items[len(p.items)-1]
	}
	return nil
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
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tabBar.Clear()
	for ind, label := range p.labels {
		fmt.Fprintf(p.tabBar, `[yellow](%s)[white] ["%d"]%s[""]  `, tabKeys[ind], ind, label)
	}
}

func (p *TabPages) Draw(screen tcell.Screen) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Flex.Draw(screen)
}

func (p *TabPages) ProcessInput(event *tcell.EventKey) *tcell.EventKey {
	key := event.Key()
	switch key {
	case tcell.KeyRune:
		for i, s := range tabKeys {
			if i >= len(p.items) {
				break
			}
			if s[0] == byte(event.Rune()) {
				p.tabBar.Highlight(strconv.Itoa(i))
				return nil
			}
		}
	}
	return event
}
