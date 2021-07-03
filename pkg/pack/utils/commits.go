// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils

import (
	"io"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
)

func getCommonTables(db objects.Store, commonCommits [][]byte) (map[string]struct{}, error) {
	commonTables := map[string]struct{}{}
	for _, b := range commonCommits {
		c, err := objects.GetCommit(db, b)
		if err != nil {
			return nil, err
		}
		commonTables[string(c.Table)] = struct{}{}
	}
	return commonTables, nil
}

func getTablesToSend(commits []*objects.Commit, buf *misc.Buffer, pw *encoding.PackfileWriter, commonTables map[string]struct{}) ([][]byte, error) {
	tables := [][]byte{}
	for _, c := range commits {
		buf.Reset()
		_, err := c.WriteTo(buf)
		if err != nil {
			return nil, err
		}
		err = pw.WriteObject(encoding.ObjectCommit, buf.Bytes())
		if err != nil {
			return nil, err
		}
		if _, ok := commonTables[string(c.Table)]; ok {
			continue
		}
		tables = append(tables, c.Table)
	}
	return tables, nil
}

func getCommonBlocks(db objects.Store, commonTables map[string]struct{}) (map[string]struct{}, error) {
	commonBlocks := map[string]struct{}{}
	for b := range commonTables {
		t, err := objects.GetTable(db, []byte(b))
		if err != nil {
			return nil, err
		}
		for _, blk := range t.Blocks {
			commonBlocks[string(blk)] = struct{}{}
		}
	}
	return commonBlocks, nil
}

func getBlocksToSend(db objects.Store, buf *misc.Buffer, pw *encoding.PackfileWriter, commonBlocks map[string]struct{}, tables [][]byte) ([][]byte, error) {
	blocks := [][]byte{}
	for _, b := range tables {
		tbl, err := objects.GetTable(db, b)
		if err != nil {
			return nil, err
		}
		for _, blk := range tbl.Blocks {
			if _, ok := commonBlocks[string(blk)]; ok {
				continue
			}
			blocks = append(blocks, blk)
		}

		buf.Reset()
		_, err = tbl.WriteTo(buf)
		if err != nil {
			return nil, err
		}
		err = pw.WriteObject(encoding.ObjectTable, buf.Bytes())
		if err != nil {
			return nil, err
		}
	}
	return blocks, nil
}

func sendBlocks(db objects.Store, pw *encoding.PackfileWriter, blocks [][]byte) error {
	for _, blk := range blocks {
		b, err := objects.GetBlockBytes(db, blk)
		if err != nil {
			return err
		}
		err = pw.WriteObject(encoding.ObjectBlock, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteCommitsToPackfile(db objects.Store, commits []*objects.Commit, commonCommits [][]byte, w io.Writer) error {
	commonTables, err := getCommonTables(db, commonCommits)
	if err != nil {
		return err
	}
	commonBlocks, err := getCommonBlocks(db, commonTables)
	if err != nil {
		return err
	}
	pw, err := encoding.NewPackfileWriter(w)
	if err != nil {
		return err
	}
	buf := misc.NewBuffer(nil)
	tables, err := getTablesToSend(commits, buf, pw, commonTables)
	if err != nil {
		return err
	}
	blocks, err := getBlocksToSend(db, buf, pw, commonBlocks, tables)
	if err != nil {
		return err
	}
	return sendBlocks(db, pw, blocks)
}
