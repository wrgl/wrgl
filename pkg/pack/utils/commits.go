// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils

import (
	"io"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func getCommonTables(db kv.DB, commonCommits [][]byte) (map[string]struct{}, error) {
	commonTables := map[string]struct{}{}
	for _, b := range commonCommits {
		c, err := versioning.GetCommit(db, b)
		if err != nil {
			return nil, err
		}
		commonTables[string(c.Table)] = struct{}{}
	}
	return commonTables, nil
}

func getTablesToSend(commits []*objects.Commit, buf *misc.Buffer, pw *encoding.PackfileWriter, commonTables map[string]struct{}) ([][]byte, error) {
	tables := [][]byte{}
	cw := objects.NewCommitWriter(buf)
	for _, c := range commits {
		buf.Reset()
		err := cw.Write(c)
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

func getCommonRows(db kv.DB, fs kv.FileStore, commonTables map[string]struct{}) (map[string]struct{}, error) {
	commonRows := map[string]struct{}{}
	for b := range commonTables {
		t, err := table.ReadTable(db, fs, []byte(b))
		if err != nil {
			return nil, err
		}
		rhr := t.NewRowHashReader(0, 0)
		for {
			_, row, err := rhr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			commonRows[string(row)] = struct{}{}
		}
	}
	return commonRows, nil
}

func getRowsToSend(db kv.DB, fs kv.FileStore, buf *misc.Buffer, pw *encoding.PackfileWriter, commonRows map[string]struct{}, tables [][]byte) ([][]byte, error) {
	tw := objects.NewTableWriter(buf)
	rows := [][]byte{}
	for _, b := range tables {
		buf.Reset()
		t, err := table.ReadTable(db, fs, []byte(b))
		if err != nil {
			return nil, err
		}
		err = tw.WriteMeta(t.Columns(), t.PrimaryKeyIndices())
		if err != nil {
			return nil, err
		}
		rhr := t.NewRowHashReader(0, 0)
		i := 0
		for {
			pk, row, err := rhr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			err = tw.WriteRowAt(append(pk, row...), i)
			if err != nil {
				return nil, err
			}
			i++
			if _, ok := commonRows[string(row)]; ok {
				continue
			}
			rows = append(rows, row)
		}
		err = tw.Flush()
		if err != nil {
			return nil, err
		}
		err = pw.WriteObject(encoding.ObjectTable, buf.Bytes())
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func sendRows(db kv.DB, pw *encoding.PackfileWriter, rows [][]byte) error {
	for _, row := range rows {
		obj, err := table.GetRow(db, row)
		if err != nil {
			return err
		}
		err = pw.WriteObject(encoding.ObjectRow, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteCommitsToPackfile(db kv.DB, fs kv.FileStore, commits []*objects.Commit, commonCommits [][]byte, w io.Writer) error {
	commonTables, err := getCommonTables(db, commonCommits)
	if err != nil {
		return err
	}
	commonRows, err := getCommonRows(db, fs, commonTables)
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
	rows, err := getRowsToSend(db, fs, buf, pw, commonRows, tables)
	if err != nil {
		return err
	}
	return sendRows(db, pw, rows)
}
