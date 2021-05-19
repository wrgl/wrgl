// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"io"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type PaginatedTextView struct {
	*tview.TextView
	lines     int
	pageSize  int
	threshold int
	pullText  func() ([]byte, error)
}

func NewPaginatedTextView() *PaginatedTextView {
	v := &PaginatedTextView{
		TextView:  tview.NewTextView().SetScrollable(true),
		pageSize:  80,
		threshold: 20,
	}
	v.TextView.SetInputCapture(v.inputCapture)
	return v
}

func (v *PaginatedTextView) PullText() error {
	b, err := v.pullText()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = v.Write(b)
	return err
}

func (v *PaginatedTextView) SetPullText(f func() ([]byte, error)) *PaginatedTextView {
	v.pullText = f
	return v
}

func (v *PaginatedTextView) SetDynamicColors(dynamic bool) *PaginatedTextView {
	v.TextView.SetDynamicColors(dynamic)
	return v
}

func (v *PaginatedTextView) SetChangedFunc(handler func()) *PaginatedTextView {
	v.TextView.SetChangedFunc(handler)
	return v
}

func (v *PaginatedTextView) Write(p []byte) (n int, err error) {
	v.lines += strings.Count(string(p), "\n")
	return v.TextView.Write(p)
}

func (v *PaginatedTextView) inputCapture(event *tcell.EventKey) *tcell.EventKey {
	key := event.Key()
	pressedDown := false
	pressedEnd := false
	switch key {
	case tcell.KeyDown:
		pressedDown = true
	case tcell.KeyEnd:
		pressedEnd = true
	case tcell.KeyRune:
		switch event.Rune() {
		case 'j':
			pressedDown = true
		case 'G':
			pressedEnd = true
		}
	}
	if pressedDown {
		row, _ := v.TextView.GetScrollOffset()
		_, _, _, height := v.Box.GetInnerRect()
		if row+height+v.threshold > v.lines {
			err := v.PullText()
			if err != nil {
				panic(err.Error())
			}
		}
	} else if pressedEnd {
		for {
			b, err := v.pullText()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err.Error())
			}
			_, err = v.Write(b)
			if err != nil {
				panic(err.Error())
			}
		}
	}
	return event
}
