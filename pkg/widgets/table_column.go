package widgets

// tableColumn manages width of a column possibly with multiple sub-columns
type tableColumn struct {
	// Total width of a column, consult this value when the cell is not divided
	Width int

	// widths of sub-columns, consult this instead when the cell is divided
	widths []int

	// Column expansion, default to 1, update to max expansion of any solo-cell
	Expansion int

	// max expansion values of sub-columns
	expansions []int
}

func newTableColumn() *tableColumn {
	return &tableColumn{
		Width: -1,
	}
}

// UpdateWidths update max widths from a possibly divided cell
func (c *tableColumn) UpdateWidths(sl []int) {
	if len(sl) == 1 {
		if sl[0] > c.Width {
			c.Width = sl[0]
		}
		return
	}
	for i, w := range sl {
		if i == len(c.widths) {
			c.widths = append(c.widths, w)
		} else if w > c.widths[i] {
			c.widths[i] = w
		}
	}
}

// UpdateExpansions update max expansion from a possibly divided cell
func (c *tableColumn) UpdateExpansions(sl []int) {
	if len(sl) == 1 {
		if sl[0] > c.Expansion {
			c.Expansion = sl[0]
		}
		return
	}
	for i, w := range sl {
		if i == len(c.expansions) {
			c.expansions = append(c.expansions, w)
		} else if w > c.expansions[i] {
			c.expansions[i] = w
		}
	}
}

func (c *tableColumn) distributeWidth(toDistribute int) {
	if toDistribute <= 0 {
		return
	}
	expansionTotal := 0
	for _, e := range c.expansions {
		expansionTotal += e
	}
	for i := range c.widths {
		expWidth := c.expansions[i] * toDistribute / expansionTotal
		c.widths[i] += expWidth
		toDistribute -= expWidth
		expansionTotal -= c.expansions[i]
	}
}

func (c *tableColumn) combinedWidths() int {
	combinedWidths := -1
	for _, w := range c.widths {
		combinedWidths += w + 1
	}
	return combinedWidths
}

// DistributeWidth distribute width among sub-columns. Call this once
// after all UpdateWidths calls.
func (c *tableColumn) DistributeWidth() {
	combinedWidths := c.combinedWidths()

	// distribute content width
	if combinedWidths > c.Width {
		c.Width = combinedWidths
	} else if combinedWidths < c.Width {
		c.distributeWidth(c.Width - combinedWidths)
	}
}

// DistributeExpansionWidth distribute expansion width among sub-columns.
// Call this once after all UpdateExpansions calls.
func (c *tableColumn) DistributeExpansionWidth(expansionWidth int) {
	c.Width += expansionWidth
	c.distributeWidth(expansionWidth)
}

func (c *tableColumn) CellWidths(l int) []int {
	if l == 1 {
		return []int{c.Width}
	}
	if l == 0 {
		return []int{-1}
	}
	return c.widths
}

func (c *tableColumn) Widths() []int {
	if len(c.widths) > 1 {
		return c.widths
	}
	return []int{c.Width}
}
