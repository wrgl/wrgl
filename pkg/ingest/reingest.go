package ingest

import (
	"bytes"
	"fmt"

	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
)

func reingestTable(db objects.Store, s *sorter.Sorter, tbl *objects.Table, opts ...InserterOption) (newTableSum []byte, err error) {
	s.Reset()
	s.SetColumns(tbl.Columns)
	s.PK, err = slice.KeyIndices(s.Columns, tbl.PrimaryKey())
	if err != nil {
		return
	}
	bb := []byte{}
	var blk [][]string
	for _, blkSum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, blkSum)
		if err != nil {
			err = fmt.Errorf("objects.GetBlock error: %v", err)
			return
		}
		for _, row := range blk {
			s.AddRow(row)
		}
	}
	inserter := NewInserter(db, s, opts...)
	return inserter.IngestTableFromSorter(s.Columns, s.PK)
}

// ReingestTable searchs for duplicated rows in a table. If duplicated rows are detected, it re-ingests the table and return the new table sum
func ReingestTable(db objects.Store, s *sorter.Sorter, tbl *objects.Table, useBlockIndex bool, opts ...InserterOption) (newTableSum []byte, err error) {
	bb := []byte{}
	if useBlockIndex && tbl.HasValidBlockIndices() {
		// search block indices for duplicated rows
		var idx *objects.BlockIndex
		var prevRowIdx = make([]byte, meow.Size)
		for _, idxSum := range tbl.BlockIndices {
			idx, bb, err = objects.GetBlockIndex(db, bb, idxSum)
			if err != nil {
				return nil, fmt.Errorf("objects.GetBlockIndex error: %v", err)
			}
			for j := 0; j < idx.Len(); j++ {
				if bytes.Equal(idx.Rows[j][:meow.Size], prevRowIdx) {
					newTableSum, err = reingestTable(db, s, tbl, opts...)
					if err != nil {
						return nil, fmt.Errorf("reingestTable error: %v", err)
					}
					return newTableSum, nil
				}
				copy(prevRowIdx, idx.Rows[j][:meow.Size])
			}
		}
	} else {
		// search blocks for duplicated rows
		var blk [][]string
		var prevRow = make([]string, len(tbl.Columns))
		for _, sum := range tbl.Blocks {
			blk, bb, err = objects.GetBlock(db, bb, sum)
			if err != nil {
				return nil, fmt.Errorf("objects.GetBlock error: %v", err)
			}
			for j := 0; j < len(blk); j++ {
				if slice.StringSliceEqual(blk[j], prevRow) {
					newTableSum, err = reingestTable(db, s, tbl, opts...)
					if err != nil {
						return nil, fmt.Errorf("reingestTable error: %v", err)
					}
					return newTableSum, nil
				}
				copy(prevRow, blk[j])
			}
		}
	}
	return nil, nil
}
