// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package widgets

import (
	"sync"
)

type CellsPool struct {
	pool  *sync.Pool
	mutex sync.Mutex
	cells map[int]map[int][]*TableCell
	t     *VirtualTable
}

// TODO: write tests

func NewCellsPool(t *VirtualTable) *CellsPool {
	p := &CellsPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return NewTableCell("")
			},
		},
		cells: map[int]map[int][]*TableCell{},
		t:     t,
	}
	t.SetOnVisibleCellsChangeFunc(p.onVisibleCellsChange)
	return p
}

func (p *CellsPool) Get(row, column, ncells int) (cells []*TableCell, found bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if mRow, ok := p.cells[row]; ok {
		if cells, ok := mRow[column]; ok {
			if len(cells) == ncells {
				return cells, true
			} else {
				// cells count have changed, discarding old cells
				for _, cell := range cells {
					cell.Reset()
					p.pool.Put(cell)
				}
			}
		}
	} else {
		p.cells[row] = map[int][]*TableCell{}
	}
	cells = make([]*TableCell, ncells)
	p.cells[row][column] = cells
	for i := 0; i < ncells; i++ {
		cells[i] = p.pool.Get().(*TableCell)
	}
	return cells, false
}

func intMap(sl []int) map[int]struct{} {
	m := map[int]struct{}{}
	for _, v := range sl {
		m[v] = struct{}{}
	}
	return m
}

func (p *CellsPool) onVisibleCellsChange(rows, columns []int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	rowsMap := intMap(rows)
	colsMap := intMap(columns)
	for row, mRow := range p.cells {
		_, keepRow := rowsMap[row]
		for col, cells := range mRow {
			_, keepCol := colsMap[col]
			if !keepRow || !keepCol {
				for _, cell := range cells {
					cell.Reset()
					p.pool.Put(cell)
				}
				delete(mRow, col)
			}
		}
		if !keepRow {
			delete(p.cells, row)
		}
	}
}
