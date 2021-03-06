// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package widgets

import (
	"github.com/gdamore/tcell/v2"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/merge"
)

var (
	resolvedCell   = NewTableCell("resolved").SetStyle(boldGreenStyle)
	unresolvedCell = NewTableCell("unresolved").SetStyle(boldStyle)
)

type MergeRow struct {
	cd      *diff.ColDiff
	Cells   [][]*TableCell
	baseRow []string
	buf     *diff.BlockBuffer
}

func NewMergeRow(buf *diff.BlockBuffer, cd *diff.ColDiff, names []string) *MergeRow {
	l := cd.Layers() + 1
	n := cd.Len() + 1
	numPK := len(cd.OtherPK[0])
	c := &MergeRow{
		buf:   buf,
		cd:    cd,
		Cells: make([][]*TableCell, l),
	}
	for i := 0; i < l; i++ {
		c.Cells[i] = make([]*TableCell, n)
		for j := 0; j < n; j++ {
			c.Cells[i][j] = NewTableCell("")
			if j == 0 && i < l-1 {
				c.Cells[i][j].SetText(names[i]).SetStyle(boldStyle)
			} else if j < numPK+1 {
				c.Cells[i][j].SetStyle(primaryKeyStyle)
			} else {
				c.Cells[i][j].SetStyle(cellStyle)
			}
		}
	}
	return c
}

func (c *MergeRow) DisplayMerge(m *merge.Merge) error {
	baseInd := len(c.Cells) - 1
	if m.Resolved {
		c.Cells[baseInd][0] = resolvedCell
	} else {
		c.Cells[baseInd][0] = unresolvedCell
	}
	if m.Base != nil {
		c.baseRow = make([]string, c.cd.Len())
		blk, off := diff.RowToBlockAndOffset(m.BaseOffset)
		row, err := c.buf.GetRow(0, blk, off)
		if err != nil {
			return err
		}
		row = c.cd.RearrangeBaseRow(row)
		for i, s := range row {
			c.baseRow[i] = s
		}
	} else {
		c.baseRow = nil
	}
	numPK := len(c.cd.OtherPK[0])
	for i, sum := range m.Others {
		cell := c.Cells[i][0]
		if sum == nil {
			cell.SetStyle(boldRedStyle)
			for j := 0; j < c.cd.Len(); j++ {
				cell := c.Cells[i][j+1]
				cell.SetText("")
				if j >= numPK {
					cell.SetStyle(cellStyle)
				}
			}
			continue
		} else if m.Base == nil {
			cell.SetStyle(boldGreenStyle)
		} else if string(sum) == string(m.Base) {
			cell.SetStyle(boldStyle)
		} else {
			cell.SetStyle(boldYellowStyle)
		}

		blk, off := diff.RowToBlockAndOffset(m.OtherOffsets[i])
		row, err := c.buf.GetRow(byte(i+1), blk, off)
		if err != nil {
			return err
		}
		row = c.cd.RearrangeRow(i, row)
		for j, s := range row {
			cell := c.Cells[i][j+1]
			cell.SetText(s)
			if j >= len(c.cd.OtherPK[0]) {
				if c.baseRow == nil {
					cell.SetStyle(greenStyle)
				} else {
					baseTxt := c.baseRow[j]
					if _, ok := c.cd.Removed[i][uint32(j)]; ok {
						cell.SetStyle(redStyle)
					} else if _, ok := c.cd.Added[i][uint32(j)]; ok {
						cell.SetStyle(greenStyle)
					} else if baseTxt != cell.Text {
						cell.SetStyle(yellowStyle)
					} else {
						cell.SetStyle(cellStyle)
					}
				}
			}
		}
	}
	if m.ResolvedRow != nil {
		for i, s := range m.ResolvedRow {
			_, ok := m.UnresolvedCols[uint32(i)]
			if m.Resolved {
				ok = false
			}
			c.SetCell(i, s, ok)
		}
	} else if c.baseRow != nil {
		for i, s := range c.baseRow {
			c.SetCell(i, s, !m.Resolved)
		}
	}
	return nil
}

func (c *MergeRow) SetCell(col int, s string, unresolved bool) {
	cell := c.Cells[c.cd.Layers()][col+1]
	cell.SetText(s)
	for i, m := range c.cd.Added {
		if _, ok := m[uint32(col)]; ok && c.Cells[i][col+1].Text == s {
			cell.SetStyle(greenStyle)
			return
		}
	}
	if col < len(c.cd.OtherPK[0]) {
		cell.SetStyle(primaryKeyStyle)
		return
	} else if c.baseRow == nil {
		cell.SetStyle(greenStyle)
	} else if s != c.baseRow[col] {
		cell.SetStyle(yellowStyle)
	} else {
		cell.SetStyle(cellStyle)
	}
	if unresolved {
		cell.SetBackgroundColor(tcell.ColorDarkRed).DisableTransparency(true)
	} else {
		cell.SetBackgroundColor(tcell.ColorBlack).DisableTransparency(false).SetTransparency(true)
	}
}

type MergeRowPool struct {
	blkBuf *diff.BlockBuffer
	cd     *diff.ColDiff
	names  []string
	buf    []*MergeRow
	rowMap map[int]int
	merges []*merge.Merge
	minInd int
	maxInd int
}

func NewMergeRowPool(blkBuf *diff.BlockBuffer, cd *diff.ColDiff, names []string, merges []*merge.Merge) *MergeRowPool {
	return &MergeRowPool{
		blkBuf: blkBuf,
		cd:     cd,
		names:  names,
		merges: merges,
		rowMap: map[int]int{},
	}
}

func (p *MergeRowPool) rowFromBuf(ind int) (*MergeRow, error) {
	n := len(p.buf)
	if c := cap(p.buf); n >= c {
		p.buf = append(p.buf, nil)
	} else {
		p.buf = p.buf[:n+1]
	}
	if p.buf[n] == nil {
		p.buf[n] = NewMergeRow(p.blkBuf, p.cd, p.names)
	}
	err := p.buf[n].DisplayMerge(p.merges[ind])
	if err != nil {
		return nil, err
	}
	if ind > p.maxInd {
		p.maxInd = ind
	} else if ind < p.minInd {
		p.minInd = ind
	}
	p.rowMap[ind] = n
	return p.buf[n], nil
}

func (p *MergeRowPool) getRow(ind int) (*MergeRow, error) {
	if ind < 0 || ind > len(p.merges) {
		return nil, nil
	}
	if v, ok := p.rowMap[ind]; ok {
		return p.buf[v], nil
	}
	if ind < p.minInd-200 || ind > p.maxInd+200 {
		p.rowMap = map[int]int{}
		p.buf = p.buf[:0]
		p.minInd = ind
		p.maxInd = ind
	}
	return p.rowFromBuf(ind)
}

func (p *MergeRowPool) SetCell(row, col int, s string, unresolved bool) {
	r, err := p.getRow(row)
	if err != nil {
		panic(err)
	}
	r.SetCell(col, s, unresolved)
}

func (p *MergeRowPool) IsTextAtCellDifferentFromBase(row, col, layer int) bool {
	r, err := p.getRow(row)
	if err != nil {
		panic(err)
	}
	return r.Cells[layer][col].Text != r.Cells[p.cd.Layers()][col].Text
}

func (p *MergeRowPool) GetCell(row, col, subrow int) *TableCell {
	r, err := p.getRow(row)
	if err != nil {
		panic(err)
	}
	return r.Cells[subrow][col]
}

func (p *MergeRowPool) RefreshRow(row int) {
	r, err := p.getRow(row)
	if err != nil {
		return
	}
	r.DisplayMerge(p.merges[row])
}
