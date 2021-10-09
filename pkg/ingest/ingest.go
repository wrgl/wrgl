package ingest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type ProgressBar interface {
	Add(int) error
	Finish() error
}

type Inserter struct {
	db          objects.Store
	pt          ProgressBar
	sortPT      ProgressBar
	tbl         *objects.Table
	tblIdx      [][]string
	wg          sync.WaitGroup
	errChan     chan error
	blocks      <-chan *sorter.Block
	numWorkers  int
	sortRunSize uint64
	debugOut    io.Writer
}

type InserterOption func(*Inserter)

func WithProgressBar(pt ProgressBar) InserterOption {
	return func(i *Inserter) {
		i.pt = pt
	}
}

func WithSortProgressBar(pt ProgressBar) InserterOption {
	return func(i *Inserter) {
		i.sortPT = pt
	}
}

func WithNumWorkers(n int) InserterOption {
	return func(i *Inserter) {
		i.numWorkers = n
	}
}

func WithSortRunSize(u uint64) InserterOption {
	return func(i *Inserter) {
		i.sortRunSize = u
	}
}

func WithDebugOutput(w io.Writer) InserterOption {
	return func(i *Inserter) {
		i.debugOut = w
	}
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
			idxSum := hash.Sum(nil)
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

func IngestTableFromBlocks(db objects.Store, columns []string, pk []uint32, rowsCount uint32, blocks <-chan *sorter.Block, opts ...InserterOption) ([]byte, error) {
	i := &Inserter{
		db:         db,
		blocks:     blocks,
		numWorkers: 1,
	}
	for _, opt := range opts {
		opt(i)
	}
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

func IngestTable(db objects.Store, f io.ReadCloser, pk []string, opts ...InserterOption) ([]byte, error) {
	i := &Inserter{
		db:         db,
		numWorkers: 1,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i.ingestTable(f, pk)
}

func (i *Inserter) ingestTable(f io.ReadCloser, pk []string) ([]byte, error) {
	s, err := sorter.NewSorter(i.sortRunSize, i.sortPT)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := s.Close(); err != nil {
			fmt.Fprint(os.Stderr, err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}()
	i.numWorkers -= 2
	if i.numWorkers <= 0 {
		i.numWorkers = 1
	}
	err = s.SortFile(f, pk)
	if err != nil {
		return nil, err
	}
	errChan := make(chan error, i.numWorkers)
	i.blocks = s.SortedBlocks(nil, errChan)
	sum, err := i.ingestTableFromBlocks(s.Columns, s.PK, s.RowsCount)
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
