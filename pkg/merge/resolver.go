package merge

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type Resolver struct {
	db      kv.DB
	cd      *objects.ColDiff
	rows    [][]string
	layers  []int
	nCols   int
	nLayers int
	rowDec  *objects.StrListDecoder
}

func NewResolver(db kv.DB, cd *objects.ColDiff) *Resolver {
	nCols := cd.Len()
	nLayers := cd.Layers()
	return &Resolver{
		db:      db,
		cd:      cd,
		nCols:   nCols,
		nLayers: nLayers,
		rows:    make([][]string, nLayers),
		layers:  make([]int, nLayers),
		rowDec:  objects.NewStrListDecoder(false),
	}
}

func (r *Resolver) decodeRow(layer int, sum []byte) ([]string, error) {
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

func (r *Resolver) mergeRows(m *Merge, uniqSums map[string]int) error {
	rows := r.rows[:0]
	layers := r.layers[:0]
	for sum, layer := range uniqSums {
		row, err := r.decodeRow(layer, []byte(sum))
		if err != nil {
			return err
		}
		rows = append(rows, row)
		layers = append(layers, layer)
	}
	if len(rows) == 1 {
		m.ResolvedRow = rows[0]
		m.Resolved = true
		return nil
	}
	baseRow, err := r.decodeRow(-1, m.Base)
	if err != nil {
		return err
	}
	resolvedRow := make([]string, r.nCols)
	var i uint32
	for i = 0; i < uint32(r.nCols); i++ {
		var add *string
		var mod *string
		var rem bool
		for j, row := range rows {
			layer := layers[j]
			if _, ok := r.cd.Added[layer][i]; ok {
				// column added in this layer
				if add == nil {
					add = &row[i]
				} else if *add != row[i] {
					// other layer add a different value
					return nil
				}
			} else if _, ok := r.cd.Removed[layer][i]; ok {
				// column removed in this layer
				if mod == nil {
					rem = true
					continue
				} else {
					// other layer modify this column
					return nil
				}
			} else if baseRow[i] != row[i] {
				// column modified in this layer
				if rem {
					// both modified and removed
					return nil
				} else if mod == nil {
					mod = &row[i]
				} else if *mod != row[i] {
					// other layer modified this column differently
					return nil
				}
			}
			resolvedRow[i] = row[i]
		}
	}
	m.ResolvedRow = resolvedRow
	m.Resolved = true
	return nil
}

func (r *Resolver) Resolve(m *Merge) (err error) {
	uniqSums := map[string]int{}
	nonNils := 0
	for i, sum := range m.Others {
		if sum != nil {
			uniqSums[string(sum)] = i
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
		return r.mergeRows(m, uniqSums)
	}
	// some modified, some removed, not resolvable
	return
}
