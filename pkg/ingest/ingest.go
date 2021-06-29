package ingest

import (
	"bytes"
	"io"
	"sync"

	"github.com/mmcloughlin/meow"
	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	"github.com/wrgl/core/pkg/objects"
)

func insertBlock(db kvcommon.DB, bar *progressbar.ProgressBar, tbl *objects.Table, tblIdx [][]string, blocks <-chan *block, wg *sync.WaitGroup, errChan chan<- error) {
	buf := bytes.NewBuffer(nil)
	w := objects.NewBlockWriter(buf)
	defer wg.Done()
	for blk := range blocks {
		// write block and add block to table
		_, err := w.Write(blk.Block)
		if err != nil {
			errChan <- err
			return
		}
		b := buf.Bytes()
		sum := meow.Checksum(0, b)
		err = kv.SaveBlock(db, sum[:], b)
		if err != nil {
			errChan <- err
			return
		}
		buf.Reset()
		tbl.Blocks[blk.Offset] = sum[:]

		// write block index and add pk sums to table index
		idx := objects.IndexBlock(blk.Block, tbl.PK)
		idx.WriteTo(buf)
		err = kv.SaveBlockIndex(db, sum[:], buf.Bytes())
		if err != nil {
			errChan <- err
			return
		}
		buf.Reset()
		tblIdx[blk.Offset] = blk.PK
		bar.Add(1)
	}
}

func IngestTable(db kvcommon.DB, name string, pk []string, sortRunSize uint64, numWorkers int, out io.Writer) ([]byte, error) {
	s, err := NewSorter(name, pk, sortRunSize, out)
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
	sum := meow.Checksum(0, buf.Bytes())
	err = kv.SaveTable(db, sum[:], buf.Bytes())
	if err != nil {
		return nil, err
	}

	// write and save table index
	buf.Reset()
	_, err = objects.NewBlockWriter(buf).Write(tblIdx)
	if err != nil {
		return nil, err
	}
	err = kv.SaveTableIndex(db, sum[:], buf.Bytes())
	if err != nil {
		return nil, err
	}

	return sum[:], nil
}
