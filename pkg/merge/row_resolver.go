// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"bytes"
	"sort"

	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/objects"
)

type RowResolver struct {
	buf     *diff.BlockBuffer
	cd      *objects.ColDiff
	rows    *Rows
	nCols   int
	nLayers int
	rowDec  *objects.StrListDecoder
}

func NewRowResolver(db objects.Store, cd *objects.ColDiff, buf *diff.BlockBuffer) *RowResolver {
	nCols := cd.Len()
	nLayers := cd.Layers()
	return &RowResolver{
		buf:     buf,
		cd:      cd,
		nCols:   nCols,
		nLayers: nLayers,
		rows:    NewRows(nLayers),
		rowDec:  objects.NewStrListDecoder(false),
	}
}

func (r *RowResolver) getRow(m *Merge, layer int) ([]string, error) {
	rowOff := m.BaseOffset
	sum := m.Base
	if layer >= 0 {
		rowOff = m.OtherOffsets[layer]
		sum = m.Others[layer]
	}
	if sum == nil {
		return nil, nil
	}
	blk, off := diff.RowToBlockAndOffset(rowOff)
	row, err := r.buf.GetRow(byte(layer+1), blk, off)
	if err != nil {
		return nil, err
	}
	if layer < 0 {
		row = r.cd.RearrangeBaseRow(row)
	} else {
		row = r.cd.RearrangeRow(layer, row)
	}
	return row, nil
}

func (r *RowResolver) tryResolve(m *Merge) (err error) {
	uniqSums := map[string]int{}
	nonNils := 0
	for i, sum := range m.Others {
		if sum != nil {
			nonNils++
			uniqSums[string(sum)] = i
		}
	}
	r.rows.Reset()
	for _, layer := range uniqSums {
		row, err := r.getRow(m, layer)
		if err != nil {
			return err
		}
		r.rows.Append(layer, row)
	}
	baseRow, err := r.getRow(m, -1)
	if err != nil {
		return
	}
	m.UnresolvedCols = map[uint32]struct{}{}
	if r.rows.Len() == 1 {
		m.ResolvedRow = r.rows.Values[0]
		m.Resolved = true
		if baseRow != nil && nonNils < len(m.Others) {
			// some modified, some removed, not resolvable
			for i, s := range baseRow {
				if s != m.ResolvedRow[i] {
					m.UnresolvedCols[uint32(i)] = struct{}{}
				}
			}
			m.Resolved = false
		} else {
			m.UnresolvedCols = nil
		}
		return
	}
	sort.Sort(r.rows)
	m.ResolvedRow = make([]string, r.nCols)
	copy(m.ResolvedRow, baseRow)
	var i uint32
	unresolveCol := func(i uint32) {
		if baseRow != nil {
			m.ResolvedRow[i] = baseRow[i]
		} else {
			m.ResolvedRow[i] = ""
		}
		m.UnresolvedCols[i] = struct{}{}
	}
	for i = 0; i < uint32(r.nCols); i++ {
		var add *string
		var mod *string
		var rem bool
		for j, row := range r.rows.Values {
			layer := r.rows.Layers[j]
			if _, ok := r.cd.Added[layer][i]; ok {
				// column added in this layer
				if add == nil {
					add = &row[i]
				} else if *add != row[i] {
					// other layer add a different value
					unresolveCol(i)
					continue
				}
			} else if add != nil {
				continue
			} else if _, ok := r.cd.Removed[layer][i]; ok {
				// column removed in this layer
				if mod == nil {
					rem = true
				} else {
					// other layer modify this column
					unresolveCol(i)
					continue
				}
			} else if baseRow == nil || baseRow[i] != row[i] {
				// column modified in this layer
				if rem {
					// modified and removed
					unresolveCol(i)
					continue
				} else if mod == nil {
					mod = &row[i]
				} else if *mod != row[i] {
					// other layer modified this column differently
					unresolveCol(i)
					continue
				}
			} else if rem || mod != nil {
				continue
			}
			m.ResolvedRow[i] = row[i]
		}
	}
	if baseRow != nil && nonNils < len(m.Others) {
		// some modified, some removed, not resolvable
		for i, s := range baseRow {
			if s != m.ResolvedRow[i] {
				m.UnresolvedCols[uint32(i)] = struct{}{}
			}
		}
		m.Resolved = false
	} else {
		m.Resolved = len(m.UnresolvedCols) == 0
		if m.Resolved {
			m.UnresolvedCols = nil
		}
	}
	return
}

func (r *RowResolver) Resolve(m *Merge) (err error) {
	nonNils := 0
	unchanges := 0
	for _, sum := range m.Others {
		if sum != nil {
			nonNils++
			if bytes.Equal(sum, m.Base) {
				unchanges++
			}
		}
	}
	if nonNils == 0 || unchanges == nonNils {
		// removed in all layers or never changed in the first place
		m.Resolved = true
		return
	}
	return r.tryResolve(m)
}
