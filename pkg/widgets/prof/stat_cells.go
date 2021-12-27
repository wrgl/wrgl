// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgetsprof

import (
	"fmt"
	"math"

	"github.com/rivo/tview"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/widgets"
)

type StatCells interface {
	Name() string
	NumRows(colProf *objects.ColumnProfile) int
	NumColumns() int
	DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*widgets.TableCell)
}

type singleStatCells struct {
	name     string
	cellText func(colProf *objects.ColumnProfile) string
}

func (c *singleStatCells) Name() string {
	return c.name
}

func (c *singleStatCells) NumRows(colProf *objects.ColumnProfile) int {
	if c.cellText(colProf) == "" {
		return 0
	}
	return 1
}

func (c *singleStatCells) NumColumns() int {
	return 1
}

func (c *singleStatCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*widgets.TableCell) {
	cells[0].SetText(c.cellText(colProf)).SetStyle(cellStyle)
}

func newSingleStatCells(name string, cellText func(colProf *objects.ColumnProfile) string) *singleStatCells {
	return &singleStatCells{
		name:     name,
		cellText: cellText,
	}
}

type topValuesCells struct {
	name   string
	values func(colProf *objects.ColumnProfile) objects.ValueCounts
}

func (c *topValuesCells) Name() string {
	return c.name
}

func (c *topValuesCells) NumRows(colProf *objects.ColumnProfile) int {
	v := c.values(colProf)
	if v == nil {
		return 0
	}
	return v.Len()
}

func (c *topValuesCells) NumColumns() int {
	return 3
}

func (c *topValuesCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*widgets.TableCell) {
	values := c.values(colProf)
	v := values[row]
	cells[0].SetText(v.Value).
		SetStyle(statNameStyle)
	cells[1].SetText(fmt.Sprintf("%d", v.Count)).
		SetStyle(cellStyle)
	pct := byte(math.Round(float64(v.Count) / float64(tblProf.RowsCount) * 100))
	cells[2].SetText(fmt.Sprintf("%3d%%", pct)).
		SetStyle(pctStyle)
}

func newTopValuesCells(name string, values func(colProf *objects.ColumnProfile) objects.ValueCounts) *topValuesCells {
	return &topValuesCells{
		name:   name,
		values: values,
	}
}

type percentilesCells struct {
	name   string
	values func(colProf *objects.ColumnProfile) []float64
}

func (c *percentilesCells) Name() string {
	return c.name
}

func (c *percentilesCells) NumRows(colProf *objects.ColumnProfile) int {
	v := c.values(colProf)
	if v == nil {
		return 0
	}
	return len(v)
}

func (c *percentilesCells) NumColumns() int {
	return 2
}

func (c *percentilesCells) DecorateCells(row int, tblProf *objects.TableProfile, colProf *objects.ColumnProfile, cells []*widgets.TableCell) {
	values := c.values(colProf)
	v := values[row]
	cells[0].SetText(fmt.Sprintf("%d", (row+1)*100/(len(values)+1))).
		SetAlign(tview.AlignRight).
		SetStyle(statNameStyle)
	cells[1].SetText(fmt.Sprintf("%f", v)).
		SetStyle(cellStyle)
}

func newPercentilesCells(name string, values func(colProf *objects.ColumnProfile) []float64) *percentilesCells {
	return &percentilesCells{
		name:   name,
		values: values,
	}
}
