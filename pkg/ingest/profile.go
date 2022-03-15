// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ingest

import (
	"bytes"
	"fmt"

	"github.com/wrgl/wrgl/pkg/dprof"
	"github.com/wrgl/wrgl/pkg/objects"
)

func ProfileTable(db objects.Store, sum []byte, tbl *objects.Table) error {
	var (
		bb       []byte
		err      error
		blk      [][]string
		profiler = dprof.NewProfiler(tbl.Columns)
	)
	for _, sum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		if err != nil {
			return fmt.Errorf("GetBlock: %v", err)
		}
		for _, row := range blk {
			profiler.Process(row)
		}
	}
	tblProf := profiler.Summarize()
	buf := bytes.NewBuffer(nil)
	_, err = tblProf.WriteTo(buf)
	if err != nil {
		return err
	}
	return objects.SaveTableProfile(db, sum, buf.Bytes())
}
