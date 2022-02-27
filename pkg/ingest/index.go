package ingest

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
)

func IndexTable(db objects.Store, tblSum []byte, tbl *objects.Table, debugOut io.Writer) error {
	var (
		tblIdx    = make([][]string, len(tbl.Blocks))
		buf       = bytes.NewBuffer(nil)
		enc       = objects.NewStrListEncoder(true)
		hash      = meow.New(0)
		blk       [][]string
		err       error
		bb        []byte
		blkIdxSum []byte
	)
	if debugOut != nil {
		fmt.Fprintf(debugOut, "Indexing table %x\n", tblSum)
	}
	var idxSum = make([]byte, meow.Size)
	for i, sum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		if err != nil {
			return fmt.Errorf("GetBlock: %v", err)
		}
		if len(tbl.PK) > 0 {
			tblIdx[i] = slice.IndicesToValues(blk[0], tbl.PK)
		} else {
			tblIdx[i] = make([]string, len(blk[0]))
			copy(tblIdx[i], blk[0])
		}
		idx, err := objects.IndexBlock(enc, hash, blk, tbl.PK)
		if err != nil {
			return fmt.Errorf("IndexBlock: %v", err)
		}
		buf.Reset()
		_, err = idx.WriteTo(buf)
		if err != nil {
			return fmt.Errorf("idx.WriteTo: %v", err)
		}
		if debugOut != nil {
			hash.Reset()
			hash.Write(buf.Bytes())
			hash.SumTo(idxSum)
			fmt.Fprintf(debugOut, "  block %x (indexSum %x)\n", sum, idxSum)
		}
		blkIdxSum, bb, err = objects.SaveBlockIndex(db, bb, buf.Bytes())
		if err != nil {
			return fmt.Errorf("objects.SaveBlockIndex: %v", err)
		}
		if !bytes.Equal(blkIdxSum, tbl.BlockIndices[i]) {
			return fmt.Errorf("block index at offset %d has different sum: %x != %x", i, blkIdxSum, tbl.BlockIndices[i])
		}
	}
	buf.Reset()
	_, err = objects.WriteBlockTo(enc, buf, tblIdx)
	if err != nil {
		return fmt.Errorf("objects.WriteBlockTo: %v", err)
	}
	return objects.SaveTableIndex(db, tblSum, buf.Bytes())
}
