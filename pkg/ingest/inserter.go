package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type asyncBlock struct {
	Offset int
	Sum    []byte
	IdxSum []byte
	PK     []string
}

type Inserter struct {
	db          objects.Store
	pt          pbar.Bar
	tbl         *objects.Table
	asyncBlocks []asyncBlock
	rowsCount   uint32
	wg          sync.WaitGroup
	errChan     chan error
	blocks      <-chan *sorter.Block
	numWorkers  int
	sorter      *sorter.Sorter
	logger      logr.Logger
}

type InserterOption func(*Inserter)

func WithProgressBar(pt pbar.Bar) InserterOption {
	return func(i *Inserter) {
		i.pt = pt
	}
}

func WithNumWorkers(n int) InserterOption {
	return func(i *Inserter) {
		i.numWorkers = n
	}
}

func NewInserter(db objects.Store, sorter *sorter.Sorter, logger logr.Logger, opts ...InserterOption) *Inserter {
	i := &Inserter{
		db:         db,
		sorter:     sorter,
		numWorkers: 1,
		logger:     logger,
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
		blkIdxSum, bb, err = objects.SaveBlockIndex(i.db, bb, buf.Bytes())
		if err != nil {
			i.errChan <- err
			return
		}
		i.logger.Info("index block", "blockSum", sum, "indexSum", blkIdxSum)
		i.asyncBlocks = append(i.asyncBlocks, asyncBlock{
			Offset: blk.Offset,
			Sum:    sum,
			IdxSum: blkIdxSum,
			PK:     blk.PK,
		})
		if i.pt != nil {
			i.pt.Incr()
		}
	}
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

func (i *Inserter) IngestTableFromSorter(columns []string, pk []uint32) ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sorterErrChan := make(chan error, 1)
	i.blocks = i.sorter.SortedBlocks(ctx, nil, sorterErrChan)
	sum, err := i.ingestTableFromBlocks(columns, pk)
	if err != nil {
		return nil, err
	}
	close(sorterErrChan)
	if err, ok := <-sorterErrChan; ok {
		return nil, err
	}
	return sum, nil
}

func ensureColumnNamesAreNotEmpty(columns []string) []string {
	m := map[string]struct{}{}
	for _, s := range columns {
		m[s] = struct{}{}
	}
	j := 1
	sl := make([]string, len(columns))
	for i, s := range columns {
		if s == "" {
			for {
				name := fmt.Sprintf("unnamed__%d", j)
				if _, ok := m[name]; !ok {
					sl[i] = name
					m[name] = struct{}{}
					break
				}
				j += 1
			}
		} else {
			sl[i] = s
		}
	}
	return sl
}

func (i *Inserter) ingestTableFromBlocks(columns []string, pk []uint32) ([]byte, error) {
	i.numWorkers -= 2
	if i.numWorkers <= 0 {
		i.numWorkers = 1
	}
	columns = ensureColumnNamesAreNotEmpty(columns)
	i.tbl = objects.NewTable(columns, pk)
	i.errChan = make(chan error, i.numWorkers)
	for j := 0; j < i.numWorkers; j++ {
		i.wg.Add(1)
		go i.insertBlock()
	}
	i.wg.Wait()
	close(i.errChan)
	err, ok := <-i.errChan
	if ok {
		return nil, err
	}
	if i.pt != nil {
		i.pt.Done()
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
	i.logger.Info("saved table", "sum", sum)

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
	if err := i.sorter.SortFile(f, pk); err != nil {
		return nil, err
	}
	return i.IngestTableFromSorter(i.sorter.Columns, i.sorter.PK)
}
