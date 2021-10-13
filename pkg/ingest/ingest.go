package ingest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type ProgressBar interface {
	Add(int) error
	Finish() error
}

type Inserter struct {
	db         objects.Store
	pt         ProgressBar
	tbl        *objects.Table
	tblIdx     [][]string
	wg         sync.WaitGroup
	errChan    chan error
	blocks     <-chan *sorter.Block
	numWorkers int
	sorter     *sorter.Sorter
	debugOut   io.Writer
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

func WithDebugOutput(w io.Writer) InserterOption {
	return func(i *Inserter) {
		i.debugOut = w
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
		i.tbl.Blocks[blk.Offset] = sum

		// write block index and add pk sums to table index
		idx, err := objects.IndexBlockFromBytes(dec, hash, e, blk.Block, i.tbl.PK)
		if err != nil {
			i.errChan <- err
			return
		}
		buf.Reset()
		idx.WriteTo(buf)
		if i.debugOut != nil {
			hash.Reset()
			hash.Write(buf.Bytes())
			hash.SumTo(idxSum)
			fmt.Fprintf(i.debugOut, "  block %x (indexSum %x)\n", sum, idxSum)
		}
		blkIdxSum, bb, err = objects.SaveBlockIndex(i.db, bb, buf.Bytes())
		if err != nil {
			i.errChan <- err
			return
		}
		i.tbl.BlockIndices[blk.Offset] = blkIdxSum
		i.tblIdx[blk.Offset] = blk.PK
		if i.pt != nil {
			i.pt.Add(1)
		}
	}
}

func IngestTableFromBlocks(db objects.Store, sorter *sorter.Sorter, columns []string, pk []uint32, rowsCount uint32, blocks <-chan *sorter.Block, opts ...InserterOption) ([]byte, error) {
	i := NewInserter(db, sorter, opts...)
	i.blocks = blocks
	return i.ingestTableFromBlocks(columns, pk, rowsCount)
}

func (i *Inserter) ingestTableFromBlocks(columns []string, pk []uint32, rowsCount uint32) ([]byte, error) {
	i.tbl = objects.NewTable(columns, pk, rowsCount)
	i.tblIdx = make([][]string, objects.BlocksCount(rowsCount))
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
	if i.debugOut != nil {
		fmt.Fprintf(i.debugOut, "Saved table %x\n", sum)
	}

	// write and save table index
	buf.Reset()
	enc := objects.NewStrListEncoder(true)
	_, err = objects.WriteBlockTo(enc, buf, i.tblIdx)
	if err != nil {
		return nil, err
	}
	err = objects.SaveTableIndex(i.db, sum, buf.Bytes())
	if err != nil {
		return nil, err
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
	go func() {
		<-c
		if err := i.sorter.Close(); err != nil {
			fmt.Fprint(os.Stderr, err.Error())
			os.Exit(1)
		}
		os.Exit(0)
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
	sum, err := i.ingestTableFromBlocks(i.sorter.Columns, i.sorter.PK, i.sorter.RowsCount)
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
