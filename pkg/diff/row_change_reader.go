package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
)

type RowChangeReader struct {
	Columns                   []*RowChangeColumn
	PKIndices                 []int
	rowIndices, oldRowIndices map[string]int
	rowPairs                  [][2]string
	off                       int
	db                        kv.DB
}

func NewRowChangeReader(db kv.DB, cols, oldCols, pk []string) (*RowChangeReader, error) {
	rowChangeCols := compareColumns(oldCols, cols)
	rowChangeCols = hoistPKTobeginning(rowChangeCols, pk)
	pkIndices, err := slice.KeyIndices(cols, pk)
	if err != nil {
		return nil, err
	}
	return &RowChangeReader{
		db:            db,
		Columns:       rowChangeCols,
		PKIndices:     pkIndices,
		rowIndices:    stringSliceToMap(cols),
		oldRowIndices: stringSliceToMap(oldCols),
	}, nil
}

func (r *RowChangeReader) AddRowPair(row, oldRow string) {
	r.rowPairs = append(r.rowPairs, [2]string{row, oldRow})
}

func (r *RowChangeReader) Read() ([][]string, error) {
	mergedRow, err := r.ReadAt(r.off)
	if err != nil {
		return nil, err
	}
	r.off++
	return mergedRow, nil
}

func (r *RowChangeReader) NumRows() int {
	return len(r.rowPairs)
}

func (r *RowChangeReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += len(r.rowPairs)
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func (r *RowChangeReader) ReadAt(offset int) (mergedRow [][]string, err error) {
	if offset >= len(r.rowPairs) {
		return nil, io.EOF
	}
	pair := r.rowPairs[offset]
	row, err := fetchRow(r.db, pair[0])
	if err != nil {
		return nil, err
	}
	oldRow, err := fetchRow(r.db, pair[1])
	if err != nil {
		return nil, err
	}
	return combineRows(r.Columns, r.rowIndices, r.oldRowIndices, row, oldRow), nil
}
