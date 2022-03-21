// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package factory

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func CommitRandomWithTable(t *testing.T, db objects.Store, tableSum []byte, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	com := &objects.Commit{
		Table:       tableSum,
		Parents:     parents,
		Time:        time.Now(),
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		Message:     testutils.BrokenRandomAlphaNumericString(10),
	}
	buf := bytes.NewBuffer(nil)
	_, err := com.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	com.Sum = sum
	return sum, com
}

func Commit(t *testing.T, db objects.Store, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	sum := BuildTable(t, db, rows, pk)
	return CommitRandomWithTable(t, db, sum, parents)
}

func CommitRandom(t *testing.T, db objects.Store, parents [][]byte) ([]byte, *objects.Commit) {
	return Commit(t, db, nil, nil, parents)
}

func CommitRandomN(t *testing.T, db objects.Store, numCols, numRows int, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	sum := BuildTableN(t, db, numCols, numRows, []uint32{0})
	return CommitRandomWithTable(t, db, sum, parents)
}

func CommitHead(t *testing.T, db objects.Store, rs ref.Store, branch string, rows []string, pk []uint32) ([]byte, *objects.Commit) {
	t.Helper()
	var parents [][]byte
	commitSum, err := ref.GetHead(rs, branch)
	if err == nil {
		parents = append(parents, commitSum)
	}
	sum, c := Commit(t, db, rows, pk, parents)
	require.NoError(t, ref.CommitHead(rs, branch, sum, c, nil))
	return sum, c
}

func CommitTag(t *testing.T, db objects.Store, rs ref.Store, tag string, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	sum, c := Commit(t, db, rows, pk, parents)
	require.NoError(t, ref.SaveTag(rs, tag, sum))
	return sum, c
}

func SdumpCommit(t *testing.T, db objects.Store, sum []byte) string {
	t.Helper()
	lines := []string{
		fmt.Sprintf("commit %x", sum),
	}
	c, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	lines = append(lines, SdumpTable(t, db, c.Table, 2))
	return strings.Join(lines, "\n")
}

func LogCommit(t *testing.T, db objects.Store, msg string, sum []byte) {
	t.Logf("%s:\n%s", msg, SdumpCommit(t, db, sum))
}

func CopyCommitsToNewStore(t *testing.T, src, dst objects.Store, commits [][]byte) {
	t.Helper()
	enc := objects.NewStrListEncoder(true)
	buf := bytes.NewBuffer(nil)
	var bb []byte
	var blk [][]string
	for _, sum := range commits {
		c, err := objects.GetCommit(src, sum)
		require.NoError(t, err)
		tbl, err := objects.GetTable(src, c.Table)
		require.NoError(t, err)
		for _, sum := range tbl.Blocks {
			blk, bb, err = objects.GetBlock(src, bb, sum)
			require.NoError(t, err)
			buf.Reset()
			_, err = objects.WriteBlockTo(enc, buf, blk)
			require.NoError(t, err)
			_, bb, err = objects.SaveBlock(dst, bb, buf.Bytes())
			require.NoError(t, err)
		}
		buf.Reset()
		_, err = tbl.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveTable(dst, buf.Bytes())
		require.NoError(t, err)
		buf.Reset()
		_, err = c.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveCommit(dst, buf.Bytes())
		require.NoError(t, err)
		require.NoError(t, ingest.IndexTable(dst, c.Table, tbl, nil))
		require.NoError(t, ingest.ProfileTable(dst, c.Table, tbl))
	}
}

func AssertTableNotPersisted(t *testing.T, db objects.Store, table []byte) {
	t.Helper()
	assert.False(t, objects.TableExist(db, table))
}

func AssertTablePersisted(t *testing.T, db objects.Store, table []byte) {
	t.Helper()
	tbl, err := objects.GetTable(db, table)
	require.NoError(t, err, "table %x not found", table)
	for _, blk := range tbl.Blocks {
		assert.True(t, objects.BlockExist(db, blk), "block %x not found", blk)
	}
	_, err = objects.GetTableIndex(db, table)
	require.NoError(t, err)
	_, err = objects.GetTableProfile(db, table)
	require.NoError(t, err)
}

func AssertTablesNotPersisted(t *testing.T, db objects.Store, tables [][]byte) {
	t.Helper()
	for _, sum := range tables {
		AssertTableNotPersisted(t, db, sum)
	}
}

func AssertTablesPersisted(t *testing.T, db objects.Store, tables [][]byte) {
	t.Helper()
	for _, sum := range tables {
		AssertTablePersisted(t, db, sum)
	}
}

func AssertCommitsPersisted(t *testing.T, db objects.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := objects.GetCommit(db, sum)
		require.NoError(t, err, "commit %x not found", sum)
		AssertTablePersisted(t, db, c.Table)
	}
}

func AssertCommitsShallowlyPersisted(t *testing.T, db objects.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		assert.True(t, objects.CommitExist(db, sum))
	}
}
