// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgetsprof

import (
	"fmt"

	"github.com/rivo/tview"
	diffprof "github.com/wrgl/wrgl/pkg/diff/prof"
	"github.com/wrgl/wrgl/pkg/widgets"
)

type StatDiffCells interface {
	Name() string
	NumRows() int
	NumColumns() int
	DecorateCells(row int, cells []*widgets.TableCell)
}

type uint16StatDiffCells struct {
	*diffprof.Uint16Stat
}

func (s *uint16StatDiffCells) Name() string {
	return s.Uint16Stat.Name
}

func (s *uint16StatDiffCells) NumRows() int {
	return 1
}

func (s *uint16StatDiffCells) NumColumns() int {
	if s.Old == s.New {
		return 1
	}
	return 2
}

func (s *uint16StatDiffCells) DecorateCells(row int, cells []*widgets.TableCell) {
	if s.Old == s.New {
		cells[0].SetText(fmt.Sprintf("%d", s.Old)).SetStyle(cellStyle)
	} else {
		if s.New != 0 {
			cells[0].SetText(fmt.Sprintf("%d", s.New)).SetStyle(addedStyle)
		}
		if s.Old != 0 {
			cells[1].SetText(fmt.Sprintf("%d", s.Old)).SetStyle(removedStyle)
		}
	}
}

type uint32StatDiffCells struct {
	*diffprof.Uint32Stat
}

func (s *uint32StatDiffCells) Name() string {
	return s.Uint32Stat.Name
}

func (s *uint32StatDiffCells) NumRows() int {
	return 1
}

func (s *uint32StatDiffCells) NumColumns() int {
	if s.Old == s.New {
		return 1
	}
	return 2
}

func (s *uint32StatDiffCells) DecorateCells(row int, cells []*widgets.TableCell) {
	if s.Old == s.New {
		cells[0].SetText(fmt.Sprintf("%d", s.Old)).SetStyle(cellStyle)
	} else {
		if s.New != 0 {
			cells[0].SetText(fmt.Sprintf("%d", s.New)).SetStyle(addedStyle)
		}
		if s.Old != 0 {
			cells[1].SetText(fmt.Sprintf("%d", s.Old)).SetStyle(removedStyle)
		}
	}
}

type float64StatDiffCells struct {
	*diffprof.Float64Stat
}

func (s *float64StatDiffCells) Name() string {
	return s.Float64Stat.Name
}

func (s *float64StatDiffCells) NumRows() int {
	return 1
}

func (s *float64StatDiffCells) NumColumns() int {
	if s.Old == s.New {
		return 1
	}
	return 2
}

func (s *float64StatDiffCells) DecorateCells(row int, cells []*widgets.TableCell) {
	if s.Old == s.New {
		cells[0].SetText(fmt.Sprintf("%d", s.Old)).SetStyle(cellStyle)
	} else {
		if s.New != nil {
			cells[0].SetText(floatString(*s.New)).SetStyle(addedStyle)
		}
		if s.Old != nil {
			cells[1].SetText(floatString(*s.Old)).SetStyle(removedStyle)
		}
	}
}

type topValuesStatDiffCells struct {
	*diffprof.TopValuesStat
}

func (s *topValuesStatDiffCells) Name() string {
	return s.TopValuesStat.Name
}

func (s *topValuesStatDiffCells) NumRows() int {
	return len(s.TopValuesStat.Values)
}

func (s *topValuesStatDiffCells) NumColumns() int {
	return 3
}

func (s *topValuesStatDiffCells) DecorateCells(row int, cells []*widgets.TableCell) {
	v := s.Values[row]
	cells[0].SetText(v.Value).
		SetStyle(statValueStyle)
	if v.NewCount == v.OldCount && v.NewPct == v.OldPct {
		cells[1].SetText(fmt.Sprintf("%d %3d%%", v.NewCount, v.NewPct)).
			SetAlign(tview.AlignRight).
			SetStyle(cellStyle)
	} else {
		if v.NewCount != 0 {
			cells[1].SetText(fmt.Sprintf("%d %3d%%", v.NewCount, v.NewPct)).
				SetAlign(tview.AlignRight).
				SetStyle(addedStyle)
		}
		if v.OldCount != 0 {
			cells[2].SetText(fmt.Sprintf("%d %3d%%", v.OldCount, v.OldPct)).
				SetAlign(tview.AlignRight).
				SetStyle(removedStyle)
		}
	}
}

type percentilesStatDiffCells struct {
	*diffprof.PercentilesStat
}

func (s *percentilesStatDiffCells) Name() string {
	return s.PercentilesStat.Name
}

func (s *percentilesStatDiffCells) NumRows() int {
	return len(s.PercentilesStat.Values)
}

func (s *percentilesStatDiffCells) NumColumns() int {
	return 3
}

func (s *percentilesStatDiffCells) DecorateCells(row int, cells []*widgets.TableCell) {
	v := s.Values[row]
	cells[0].SetText(fmt.Sprintf("%d", (row+1)*100/(len(s.Values)+1))).
		SetStyle(statValueStyle)
	if v.New == v.Old {
		cells[1].SetText(floatString(v.New)).
			SetAlign(tview.AlignRight).
			SetStyle(cellStyle)
	} else {
		if v.New != 0 {
			cells[1].SetText(floatString(v.New)).
				SetAlign(tview.AlignRight).
				SetStyle(addedStyle)
		}
		if v.Old != 0 {
			cells[2].SetText(floatString(v.Old)).
				SetAlign(tview.AlignRight).
				SetStyle(removedStyle)
		}
	}
}
