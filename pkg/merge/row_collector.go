// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"

	"github.com/wrgl/core/pkg/index"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type RowCollector struct {
	discardedRows *index.HashSet
	resolvedRows  *SortableRows
	cd            *objects.ColDiff
	cols          []int
	db            kv.DB
	baseT         table.Store
}

func NewCollector(db kv.DB, baseT table.Store, resolvedRows *SortableRows, discardedRow *index.HashSet) *RowCollector {
	c := &RowCollector{
		db:            db,
		discardedRows: discardedRow,
		resolvedRows:  resolvedRows,
		baseT:         baseT,
	}
	return c
}

func (c *RowCollector) collectColumnIndices() {
	cols := []int{}
	layers := c.cd.Layers()
mainLoop:
	for i := range c.cd.Names {
		for layer := 0; layer < layers; layer++ {
			if _, ok := c.cd.Removed[layer][uint32(i)]; ok {
				continue mainLoop
			}
		}
		cols = append(cols, i)
	}
	c.cols = cols
}

func (c *RowCollector) SaveResolvedRow(pk []byte, row []string) error {
	if row != nil {
		vals := make([]string, len(c.cols))
		for i, j := range c.cols {
			vals[i] = row[j]
		}
		err := c.resolvedRows.Add(vals)
		if err != nil {
			return err
		}
	}
	return c.discardedRows.Add(pk)
}

func (c *RowCollector) CollectResolvedRow(errChan chan<- error, origChan <-chan Merge) <-chan Merge {
	mergeChan := make(chan Merge)
	go func() {
		defer close(mergeChan)
		for m := range origChan {
			if m.ColDiff != nil {
				c.cd = m.ColDiff
				c.collectColumnIndices()
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

func (c *RowCollector) Columns() []string {
	vals := make([]string, len(c.cols))
	for i, j := range c.cols {
		vals[i] = c.cd.Names[j]
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
	rhr := c.baseT.NewRowHashReader(0, 0)
	for {
		pk, sum, err := rhr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ok, err := c.discardedRows.Has(pk)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		b, err := table.GetRow(c.db, sum)
		if err != nil {
			return err
		}
		err = c.resolvedRows.AddBytes(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RowCollector) SortedRows(errChan chan<- error) (<-chan []string, error) {
	err := c.collectRowsThatStayedTheSame()
	if err != nil {
		return nil, err
	}
	sortBy := make([]int, len(c.cd.OtherPK[0]))
	for i, j := range c.cd.OtherPK[0] {
		sortBy[i] = int(j) + 1
	}
	err = c.resolvedRows.SetSortBy(sortBy)
	if err != nil {
		return nil, err
	}
	sort.Sort(c.resolvedRows)
	return c.resolvedRows.RowsChan(errChan), nil
}

func (c *RowCollector) Close() error {
	err := c.discardedRows.Close()
	if err != nil {
		return err
	}
	return c.resolvedRows.Close()
}

func CreateRowCollector(db kv.DB, baseT table.Store) (collector *RowCollector, cleanup func(), err error) {
	rowsFile, err := ioutil.TempFile("", "rows_")
	if err != nil {
		return
	}
	offFile, err := ioutil.TempFile("", "off_")
	if err != nil {
		return
	}
	resolvedRows, err := NewSortableRows(rowsFile, offFile, nil)
	if err != nil {
		return
	}
	hashSetFile, err := ioutil.TempFile("", "hashset_")
	if err != nil {
		return
	}
	discardedRows, err := index.NewHashSet(hashSetFile, 0)
	if err != nil {
		return
	}
	collector = NewCollector(db, baseT, resolvedRows, discardedRows)
	cleanup = func() {
		collector.Close()
		os.Remove(rowsFile.Name())
		os.Remove(offFile.Name())
		os.Remove(hashSetFile.Name())
	}
	return
}
