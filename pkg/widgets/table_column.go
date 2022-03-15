// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package widgets

// tableColumn manages width of a column possibly with multiple sub-columns
type tableColumn struct {
	// row indices at which number of sub-columns changes
	rows []int

	// Total width of a column, consult this value when the cell is not divided
	Width int

	// widths of sub-columns, consult this instead when the cell is divided
	widths [][]int

	// max expansion values of sub-columns
	expansions [][]int
}

func newTableColumn() *tableColumn {
	return &tableColumn{
		Width: -1,
	}
}

func (c *tableColumn) getBlockIndex(row int, subCols int) int {
	if len(c.rows) == 0 {
		c.rows = append(c.rows, 0)
		c.widths = append(c.widths, make([]int, subCols))
		c.expansions = append(c.expansions, make([]int, subCols))
		return 0
	}
	var ind int
	for i, j := range c.rows {
		if j > row {
			break
		}
		ind = i
	}
	if len(c.widths[ind]) == subCols {
		return ind
	}
	c.rows = append(c.rows, row)
	c.widths = append(c.widths, make([]int, subCols))
	c.expansions = append(c.expansions, make([]int, subCols))
	return ind + 1
}

// UpdateWidths update max widths from a possibly divided cell
func (c *tableColumn) UpdateWidths(row int, sl []int) {
	ind := c.getBlockIndex(row, len(sl))
	for i, w := range sl {
		if w > c.widths[ind][i] {
			c.widths[ind][i] = w
		}
	}
}

// UpdateExpansions update max expansion from a possibly divided cell
func (c *tableColumn) UpdateExpansions(row int, sl []int) {
	ind := c.getBlockIndex(row, len(sl))
	for i, w := range sl {
		if w > c.expansions[ind][i] {
			c.expansions[ind][i] = w
		}
	}
}

func (c *tableColumn) distributeWidth(ind, toDistribute int) {
	widths := c.widths[ind]
	expansionTotal := 0
	for _, e := range c.expansions[ind] {
		expansionTotal += e
	}
	for i := range widths {
		if expansionTotal > 0 {
			expWidth := c.expansions[ind][i] * toDistribute / expansionTotal
			widths[i] += expWidth
			toDistribute -= expWidth
			expansionTotal -= c.expansions[ind][i]
		} else {
			widths[i] += toDistribute / len(widths)
		}
	}
}

// DistributeWidth distribute width among sub-columns. Call this once
// after all UpdateWidths calls.
func (c *tableColumn) DistributeWidth() {
	c.Width = 0
	for _, widths := range c.widths {
		cw := -1
		for _, w := range widths {
			cw += w + 1
		}
		if cw > c.Width {
			c.Width = cw
		}
	}
	for i, widths := range c.widths {
		cw := -1
		for _, w := range widths {
			cw += w + 1
		}
		if cw < c.Width {
			c.distributeWidth(i, c.Width-cw)
		}
	}
}

// DistributeExpansionWidth distribute expansion width among sub-columns.
// Call this once after all UpdateExpansions calls.
func (c *tableColumn) DistributeExpansionWidth(expansionWidth int) {
	c.Width += expansionWidth
	for i := range c.widths {
		c.distributeWidth(i, expansionWidth)
	}
}

func (c *tableColumn) Widths(row int) []int {
	var ind int
	for i, j := range c.rows {
		if j > row {
			break
		}
		ind = i
	}
	return c.widths[ind]
}
