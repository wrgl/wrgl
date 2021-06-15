// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"sort"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type RowResolver struct {
	db      kv.DB
	cd      *objects.ColDiff
	rows    *Rows
	nCols   int
	nLayers int
	rowDec  *objects.StrListDecoder
}

func NewRowResolver(db kv.DB, cd *objects.ColDiff) *RowResolver {
	nCols := cd.Len()
	nLayers := cd.Layers()
	return &RowResolver{
		db:      db,
		cd:      cd,
		nCols:   nCols,
		nLayers: nLayers,
		rows:    NewRows(nLayers),
		rowDec:  objects.NewStrListDecoder(false),
	}
}

func (r *RowResolver) decodeRow(layer int, sum []byte) ([]string, error) {
	if sum == nil {
		return nil, nil
	}
	b, err := table.GetRow(r.db, []byte(sum))
	if err != nil {
		return nil, err
	}
	row := r.rowDec.Decode(b)
	if layer < 0 {
		row = r.cd.RearrangeBaseRow(row)
	} else {
		row = r.cd.RearrangeRow(layer, row)
	}
	return row, nil
}

func (r *RowResolver) TryResolve(m *Merge) (resolvedRow []string, resolved bool, err error) {
	uniqSums := map[string]int{}
	for i, sum := range m.Others {
		if sum != nil {
			uniqSums[string(sum)] = i
		}
	}
	r.rows.Reset()
	for sum, layer := range uniqSums {
		row, err := r.decodeRow(layer, []byte(sum))
		if err != nil {
			return nil, false, err
		}
		r.rows.Append(layer, row)
	}
	if r.rows.Len() == 1 {
		resolvedRow = r.rows.Values[0]
		resolved = true
		return
	}
	sort.Sort(r.rows)
	baseRow, err := r.decodeRow(-1, m.Base)
	if err != nil {
		return
	}
	resolvedRow = make([]string, r.nCols)
	copy(resolvedRow, baseRow)
	var i uint32
	m.UnresolvedCols = map[uint32]struct{}{}
	unresolveCol := func(i uint32) {
		if baseRow != nil {
			resolvedRow[i] = baseRow[i]
		} else {
			resolvedRow[i] = ""
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
			resolvedRow[i] = row[i]
		}
	}
	resolved = len(m.UnresolvedCols) == 0
	if resolved {
		m.UnresolvedCols = nil
	}
	return
}

func (r *RowResolver) mergeRows(m *Merge) (err error) {
	resolvedRow, resolved, err := r.TryResolve(m)
	if err != nil {
		return err
	}
	m.Resolved = resolved
	m.ResolvedRow = resolvedRow
	return nil
}

func (r *RowResolver) Resolve(m *Merge) (err error) {
	nonNils := 0
	for _, sum := range m.Others {
		if sum != nil {
			nonNils++
		}
	}
	if nonNils == 0 {
		// removed in all layers or never changed in the first place
		m.Resolved = true
		return
	} else if m.Base == nil || nonNils == r.nLayers {
		// row was added in one or more layers
		// or all layers modified this row
		return r.mergeRows(m)
	}
	// some modified, some removed, not resolvable
	return
}
