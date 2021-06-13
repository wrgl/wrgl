package widgets

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type MergeRow struct {
	db      kv.DB
	cd      *objects.ColDiff
	Cells   [][]*TableCell
	baseRow []string
	dec     *objects.StrListDecoder
}

func NewMergeRow(db kv.DB, dec *objects.StrListDecoder, cd *objects.ColDiff, names []string) *MergeRow {
	l := cd.Layers() + 1
	n := cd.Len() + 1
	numPK := len(cd.OtherPK[0])
	c := &MergeRow{
		db:    db,
		dec:   dec,
		cd:    cd,
		Cells: make([][]*TableCell, l),
	}
	for i := 0; i < l; i++ {
		c.Cells[i] = make([]*TableCell, n)
		for j := 0; j < n; j++ {
			c.Cells[i][j] = NewTableCell("")
			if j == 0 {
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
	c.baseRow = make([]string, c.cd.Len())
	if m.Base != nil {
		rowB, err := table.GetRow(c.db, m.Base)
		if err != nil {
			return err
		}
		row := c.cd.RearrangeBaseRow(c.dec.Decode(rowB))
		for i, s := range row {
			c.Cells[baseInd][i+1].SetText(s)
			c.baseRow[i] = s
		}
		c.Cells[baseInd][0].SetStyle(boldStyle)
	} else {
		c.Cells[baseInd][0].SetStyle(boldRedStyle)
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

		rowB, err := table.GetRow(c.db, sum)
		if err != nil {
			return err
		}
		row := c.cd.RearrangeRow(i, c.dec.Decode(rowB))
		for j, s := range row {
			cell := c.Cells[i][j+1]
			cell.SetText(s)
			if j >= len(c.cd.OtherPK[0]) {
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
	return nil
}

func (c *MergeRow) SetCell(col int, s string) {
	cell := c.Cells[c.cd.Layers()][col+1]
	cell.SetText(s)
	for i, m := range c.cd.Added {
		if _, ok := m[uint32(col)]; ok && c.Cells[i][col+1].Text == s {
			cell.SetStyle(greenStyle)
			return
		}
	}
	if s != c.baseRow[col] {
		cell.SetStyle(yellowStyle)
	} else {
		cell.SetStyle(cellStyle)
	}
}

func (c *MergeRow) SetCellFromLayer(col, layer int) {
	c.SetCell(col, c.Cells[layer][col+1].Text)
}

type MergeRowPool struct {
	db     kv.DB
	cd     *objects.ColDiff
	dec    *objects.StrListDecoder
	names  []string
	buf    []*MergeRow
	rowMap map[int]int
	merges []*merge.Merge
	minInd int
	maxInd int
}

func NewMergeRowPool(db kv.DB, cd *objects.ColDiff, names []string, merges []*merge.Merge) *MergeRowPool {
	return &MergeRowPool{
		db:     db,
		cd:     cd,
		dec:    objects.NewStrListDecoder(false),
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
		p.buf[n] = NewMergeRow(p.db, p.dec, p.cd, p.names)
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

func (p *MergeRowPool) GetRow(ind int) (*MergeRow, error) {
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
