package ingest

import (
	"bytes"

	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

func IndexTable(db kvcommon.DB, tblSum []byte, tbl *objects.Table) error {
	tblIdx := make([][]string, len(tbl.Blocks))
	buf := bytes.NewBuffer(nil)
	for i, sum := range tbl.Blocks {
		b, err := kv.GetBlock(db, sum)
		if err != nil {
			return err
		}
		_, blk, err := objects.ReadBlockFrom(bytes.NewReader(b))
		if err != nil {
			return err
		}
		tblIdx[i] = slice.IndicesToValues(blk[0], tbl.PK)
		idx := objects.IndexBlock(blk, tbl.PK)
		_, err = idx.WriteTo(buf)
		if err != nil {
			return err
		}
		err = kv.SaveBlockIndex(db, sum, buf.Bytes())
		if err != nil {
			return err
		}
		buf.Reset()
	}
	_, err := objects.WriteBlockTo(buf, tblIdx)
	if err != nil {
		return err
	}
	return kv.SaveTableIndex(db, tblSum, buf.Bytes())
}
