// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package merge

import (
	"fmt"
	"os"

	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/index"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

type RowCollector struct {
	discardedRows *index.HashSet
	resolvedRows  *sorter.Sorter
	cd            *diff.ColDiff
	db            objects.Store
	baseT         *objects.Table
}

func NewCollector(db objects.Store, baseT *objects.Table, discardedRow *index.HashSet) (*RowCollector, error) {
	s, err := sorter.NewSorter()
	if err != nil {
		return nil, err
	}
	s.PK = baseT.PK
	c := &RowCollector{
		db:            db,
		discardedRows: discardedRow,
		resolvedRows:  s,
		baseT:         baseT,
	}
	return c, nil
}

func (c *RowCollector) SaveResolvedRow(pk []byte, row []string) error {
	if row != nil {
		err := c.resolvedRows.AddRow(row)
		if err != nil {
			return err
		}
	}
	return c.discardedRows.Add(pk)
}

func (c *RowCollector) CollectResolvedRow(errChan chan<- error, origChan <-chan *Merge) <-chan *Merge {
	mergeChan := make(chan *Merge)
	go func() {
		defer close(mergeChan)
		for m := range origChan {
			if m.ColDiff != nil {
				c.cd = m.ColDiff
			} else if m.Resolved {
				err := c.SaveResolvedRow(m.PK, m.ResolvedRow)
				if err != nil {
					errChan <- fmt.Errorf("save resolved row error: %v", err)
					return
				}
				// don't emit resolved row
				continue
			}
			mergeChan <- m
		}
	}()
	return mergeChan
}

func (c *RowCollector) Columns(removedCols map[int]struct{}) []string {
	vals := make([]string, 0, c.cd.Len()-len(removedCols))
	for i, s := range c.cd.Names {
		if _, ok := removedCols[i]; ok {
			continue
		}
		vals = append(vals, s)
	}
	return vals
}

func (c *RowCollector) PK() []string {
	return c.cd.PK()
}

func (c *RowCollector) collectRowsThatStayedTheSame() error {
	err := c.discardedRows.Flush()
	if err != nil {
		return err
	}
	enc := objects.NewStrListEncoder(true)
	hash := meow.New(0)
	var buf []byte
	var blk [][]string
	pk := make([]byte, 16)
	for _, sum := range c.baseT.Blocks {
		blk, buf, err = objects.GetBlock(c.db, buf, sum)
		if err != nil {
			return err
		}
		for _, row := range blk {
			hash.Reset()
			_, err := hash.Write(enc.Encode(slice.IndicesToValues(row, c.baseT.PK)))
			if err != nil {
				return err
			}
			hash.SumTo(pk)
			ok, err := c.discardedRows.Has(pk)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			err = c.resolvedRows.AddRow(row)
			if err != nil {
				return err
			}

		}
	}
	return nil
}

func (c *RowCollector) SortedBlocks(removedCols map[int]struct{}, errChan chan<- error) (<-chan *sorter.Block, error) {
	err := c.collectRowsThatStayedTheSame()
	if err != nil {
		return nil, err
	}
	return c.resolvedRows.SortedBlocks(removedCols, errChan), nil
}

func (c *RowCollector) SortedRows(removedCols map[int]struct{}, errChan chan<- error) (<-chan *sorter.Rows, error) {
	err := c.collectRowsThatStayedTheSame()
	if err != nil {
		return nil, err
	}
	return c.resolvedRows.SortedRows(removedCols, errChan), nil
}

func (c *RowCollector) Close() error {
	err := c.discardedRows.Close()
	if err != nil {
		return err
	}
	return c.resolvedRows.Close()
}

func CreateRowCollector(db objects.Store, baseT *objects.Table) (collector *RowCollector, cleanup func(), err error) {
	hashSetFile, err := testutils.TempFile("", "hashset_")
	if err != nil {
		return
	}
	discardedRows, err := index.NewHashSet(hashSetFile, 0)
	if err != nil {
		return
	}
	collector, err = NewCollector(db, baseT, discardedRows)
	if err != nil {
		return
	}
	cleanup = func() {
		collector.Close()
		os.Remove(hashSetFile.Name())
	}
	return
}
