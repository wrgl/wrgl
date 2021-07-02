package ingest

import (
	"bytes"
	"io"
	"sync"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/core/pkg/objects"
)

func insertBlock(db objects.Store, bar *progressbar.ProgressBar, tbl *objects.Table, tblIdx [][]string, blocks <-chan *block, wg *sync.WaitGroup, errChan chan<- error) {
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

func IngestTable(db objects.Store, f io.ReadCloser, name string, pk []string, sortRunSize uint64, numWorkers int, out io.Writer) ([]byte, error) {
	s, err := NewSorter(f, name, pk, sortRunSize, out)
	if err != nil {
		return nil, err
	}
	numWorkers -= 2
	if numWorkers <= 0 {
		numWorkers = 1
	}
	errChan := make(chan error, numWorkers+1)
	blocks := s.EmitChunks(errChan)
	bar := pbar(-1, "saving blocks", out)
	tbl := objects.NewTable(s.Columns, s.PK, s.RowsCount)
	tblIdx := make([][]string, objects.BlocksCount(s.RowsCount))
	wg := &sync.WaitGroup{}
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
