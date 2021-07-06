package ingest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/sorter"
)

func insertBlock(db objects.Store, bar *progressbar.ProgressBar, tbl *objects.Table, tblIdx [][]string, blocks <-chan *sorter.Block, wg *sync.WaitGroup, errChan chan<- error) {
	buf := bytes.NewBuffer(nil)
	defer wg.Done()
	for blk := range blocks {
		// write block and add block to table
		buf.Reset()
		_, err := objects.WriteBlockTo(buf, blk.Block)
		if err != nil {
			errChan <- err
			return
		}
		sum, err := objects.SaveBlock(db, buf.Bytes())
		if err != nil {
			errChan <- err
			return
		}
		tbl.Blocks[blk.Offset] = sum

		// write block index and add pk sums to table index
		idx := objects.IndexBlock(blk.Block, tbl.PK)
		buf.Reset()
		idx.WriteTo(buf)
		err = objects.SaveBlockIndex(db, sum, buf.Bytes())
		if err != nil {
			errChan <- err
			return
		}
		tblIdx[blk.Offset] = blk.PK
		bar.Add(1)
	}
}

func IngestTableFromBlocks(db objects.Store, columns []string, pk []uint32, rowsCount uint32, blocks <-chan *sorter.Block, numWorkers int, out io.Writer) ([]byte, error) {
	bar := pbar(-1, "saving blocks", out)
	tbl := objects.NewTable(columns, pk, rowsCount)
	tblIdx := make([][]string, objects.BlocksCount(rowsCount))
	wg := &sync.WaitGroup{}
	errChan := make(chan error, numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go insertBlock(db, bar, tbl, tblIdx, blocks, wg, errChan)
	}
	wg.Wait()
	close(errChan)
	err, ok := <-errChan
	if ok {
		return nil, err
	}
	err = bar.Finish()
	if err != nil {
		return nil, err
	}

	// write and save table
	buf := bytes.NewBuffer(nil)
	_, err = tbl.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	sum, err := objects.SaveTable(db, buf.Bytes())
	if err != nil {
		return nil, err
	}

	// write and save table index
	buf.Reset()
	_, err = objects.WriteBlockTo(buf, tblIdx)
	if err != nil {
		return nil, err
	}
	err = objects.SaveTableIndex(db, sum, buf.Bytes())
	if err != nil {
		return nil, err
	}

	return sum, nil
}

func IngestTable(db objects.Store, f io.ReadCloser, pk []string, sortRunSize uint64, numWorkers int, out io.Writer) ([]byte, error) {
	s, err := sorter.NewSorter(sortRunSize, out)
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
	numWorkers -= 2
	if numWorkers <= 0 {
		numWorkers = 1
	}
	errChan := make(chan error, numWorkers)
	blocks, err := s.SortFile(f, pk, errChan)
	if err != nil {
		return nil, err
	}
	close(errChan)
	err, ok := <-errChan
	if ok {
		return nil, err
	}
	return IngestTableFromBlocks(db, s.Columns, s.PK, s.RowsCount, blocks, numWorkers, out)
}
