// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ingest

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"

	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type ProgressBar interface {
	Add(int) error
	Finish() error
}

type asyncBlock struct {
	Offset int
	Sum    []byte
	IdxSum []byte
	PK     []string
}

type Inserter struct {
	db          objects.Store
	pt          ProgressBar
	tbl         *objects.Table
	asyncBlocks []asyncBlock
	rowsCount   uint32
	wg          sync.WaitGroup
	errChan     chan error
	blocks      <-chan *sorter.Block
	numWorkers  int
	sorter      *sorter.Sorter
	debugLogger *log.Logger
}

type InserterOption func(*Inserter)

func WithProgressBar(pt ProgressBar) InserterOption {
	return func(i *Inserter) {
		i.pt = pt
	}
}

func WithNumWorkers(n int) InserterOption {
	return func(i *Inserter) {
		i.numWorkers = n
	}
}

func WithDebugLogger(w *log.Logger) InserterOption {
	return func(i *Inserter) {
		i.debugLogger = w
	}
}

func NewInserter(db objects.Store, sorter *sorter.Sorter, opts ...InserterOption) *Inserter {
	i := &Inserter{
		db:         db,
		sorter:     sorter,
		numWorkers: 1,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

func (i *Inserter) insertBlock() {
	var (
		sum       []byte
		blkIdxSum []byte
		err       error
		buf       = bytes.NewBuffer(nil)
		bb        []byte
		dec       = objects.NewStrListDecoder(true)
		hash      = meow.New(0)
		e         = objects.NewStrListEditor(i.tbl.PK)
		idxSum    = make([]byte, meow.Size)
	)
	defer i.wg.Done()
	for blk := range i.blocks {
		// write block and add block to table
		sum, bb, err = objects.SaveBlock(i.db, bb, blk.Block)
		if err != nil {
			i.errChan <- err
			return
		}
		i.rowsCount += uint32(blk.RowsCount)

		// write block index and add pk sums to table index
		idx, err := objects.IndexBlockFromBytes(dec, hash, e, blk.Block, i.tbl.PK)
		if err != nil {
			i.errChan <- err
			return
		}
		buf.Reset()
		idx.WriteTo(buf)
		if i.debugLogger != nil {
			hash.Reset()
			hash.Write(buf.Bytes())
			hash.SumTo(idxSum)
			i.debugLogger.Printf("  block %x (indexSum %x)\n", sum, idxSum)
		}
		blkIdxSum, bb, err = objects.SaveBlockIndex(i.db, bb, buf.Bytes())
		if err != nil {
			i.errChan <- err
			return
		}
		i.asyncBlocks = append(i.asyncBlocks, asyncBlock{
			Offset: blk.Offset,
			Sum:    sum,
			IdxSum: blkIdxSum,
			PK:     blk.PK,
		})
		if i.pt != nil {
			i.pt.Add(1)
		}
	}
}

func IngestTableFromBlocks(db objects.Store, sorter *sorter.Sorter, columns []string, pk []uint32, blocks <-chan *sorter.Block, opts ...InserterOption) ([]byte, error) {
	i := NewInserter(db, sorter, opts...)
	i.blocks = blocks
	return i.ingestTableFromBlocks(columns, pk)
}

func (o *Inserter) sortBlocks() (blkPKs [][]string) {
	n := len(o.asyncBlocks)
	sort.Slice(o.asyncBlocks, func(i, j int) bool {
		return o.asyncBlocks[i].Offset < o.asyncBlocks[j].Offset
	})
	o.tbl.Blocks = make([][]byte, n)
	o.tbl.BlockIndices = make([][]byte, n)
	blkPKs = make([][]string, n)
	for i, blk := range o.asyncBlocks {
		o.tbl.Blocks[i] = blk.Sum
		o.tbl.BlockIndices[i] = blk.IdxSum
		blkPKs[i] = blk.PK
	}
	return
}

func (i *Inserter) ingestTableFromBlocks(columns []string, pk []uint32) ([]byte, error) {
	for i, s := range columns {
		if s == "" {
			return nil, &Error{fmt.Sprintf("column name at position %d is empty", i)}
		}
	}
	i.tbl = objects.NewTable(columns, pk)
	errChan := make(chan error, i.numWorkers)
	for j := 0; j < i.numWorkers; j++ {
		i.wg.Add(1)
		go i.insertBlock()
	}
	i.wg.Wait()
	close(errChan)
	err, ok := <-errChan
	if ok {
		return nil, err
	}
	if i.pt != nil {
		if err := i.pt.Finish(); err != nil {
			return nil, err
		}
	}
	tblIdx := i.sortBlocks()
	i.tbl.RowsCount = i.rowsCount

	// write and save table
	buf := bytes.NewBuffer(nil)
	_, err = i.tbl.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	sum, err := objects.SaveTable(i.db, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if i.debugLogger != nil {
		i.debugLogger.Printf("Saved table %x\n", sum)
	}

	// write and save table index
	buf.Reset()
	enc := objects.NewStrListEncoder(true)
	_, err = objects.WriteBlockTo(enc, buf, tblIdx)
	if err != nil {
		return nil, err
	}
	err = objects.SaveTableIndex(i.db, sum, buf.Bytes())
	if err != nil {
		return nil, err
	}

	// write and save table summary
	buf.Reset()
	ts := i.sorter.TableSummary()
	if ts != nil {
		_, err = ts.WriteTo(buf)
		if err != nil {
			return nil, err
		}
		err = objects.SaveTableProfile(i.db, sum, buf.Bytes())
		if err != nil {
			return nil, err
		}
	}

	return sum, nil
}

func IngestTable(db objects.Store, sorter *sorter.Sorter, f io.ReadCloser, pk []string, opts ...InserterOption) ([]byte, error) {
	i := NewInserter(db, sorter, opts...)
	return i.ingestTable(f, pk)
}

func (i *Inserter) ingestTable(f io.ReadCloser, pk []string) ([]byte, error) {
	defer i.sorter.Close()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	done := make(chan bool)
	go func() {
		select {
		case <-c:
			if err := i.sorter.Close(); err != nil {
				fmt.Fprint(os.Stderr, err.Error())
				os.Exit(1)
			}
			os.Exit(0)
		case <-done:
			return
		}
	}()
	defer func() {
		done <- true
		close(done)
	}()
	i.numWorkers -= 2
	if i.numWorkers <= 0 {
		i.numWorkers = 1
	}
	if err := i.sorter.SortFile(f, pk); err != nil {
		return nil, err
	}
	errChan := make(chan error, i.numWorkers)
	i.blocks = i.sorter.SortedBlocks(nil, errChan)
	sum, err := i.ingestTableFromBlocks(i.sorter.Columns, i.sorter.PK)
	if err != nil {
		return nil, err
	}
	close(errChan)
	err, ok := <-errChan
	if ok {
		return nil, err
	}
	return sum, nil
}
