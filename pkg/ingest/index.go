package ingest

import (
	"bytes"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

func IndexTable(db objects.Store, tblSum []byte, tbl *objects.Table) error {
	tblIdx := make([][]string, len(tbl.Blocks))
	buf := bytes.NewBuffer(nil)
	enc := objects.NewStrListEncoder(true)
	hash := meow.New(0)
	for i, sum := range tbl.Blocks {
		blk, err := objects.GetBlock(db, sum)
		if err != nil {
			return err
		}
		tblIdx[i] = slice.IndicesToValues(blk[0], tbl.PK)
		idx, err := objects.IndexBlock(enc, hash, blk, tbl.PK)
		if err != nil {
			return err
		}
		_, err = idx.WriteTo(buf)
		if err != nil {
			return err
		}
		err = objects.SaveBlockIndex(db, sum, buf.Bytes())
		if err != nil {
			return err
		}
		buf.Reset()
	}
	_, err := objects.WriteBlockTo(enc, buf, tblIdx)
	if err != nil {
		return err
	}
	return objects.SaveTableIndex(db, tblSum, buf.Bytes())
}
