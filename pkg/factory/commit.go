// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package factory

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvfs "github.com/wrgl/core/pkg/kv/fs"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
)

func Commit(t *testing.T, db kvcommon.DB, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	c := &objects.Commit{
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		Message:     testutils.BrokenRandomAlphaNumericString(20),
		Time:        time.Now(),
		Parents:     parents,
	}
	sum := BuildTable(t, db, rows, pk)
	c.Table = sum
	buf := bytes.NewBuffer(nil)
	_, err := c.WriteTo(buf)
	require.NoError(t, err)
	arr := meow.Checksum(0, buf.Bytes())
	require.NoError(t, kv.SaveCommit(db, arr[:], buf.Bytes()))
	return sum, c
}

func CommitRandom(t *testing.T, db kvcommon.DB, parents [][]byte) ([]byte, *objects.Commit) {
	return Commit(t, db, nil, nil, parents)
}

func CommitHead(t *testing.T, db kvcommon.DB, fs kvfs.FileStore, branch string, rows []string, pk []uint32) ([]byte, *objects.Commit) {
	t.Helper()
	var parents [][]byte
	commitSum, err := ref.GetHead(db, branch)
	if err == nil {
		parents = append(parents, commitSum)
	}
	sum, c := Commit(t, db, rows, pk, parents)
	require.NoError(t, ref.CommitHead(db, fs, branch, sum, c))
	return sum, c
}

func CommitTag(t *testing.T, db kvcommon.DB, tag string, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	sum, c := Commit(t, db, rows, pk, parents)
	require.NoError(t, ref.SaveTag(db, tag, sum))
	return sum, c
}

func SdumpCommit(t *testing.T, db kvcommon.DB, sum []byte) string {
	t.Helper()
	lines := []string{
		fmt.Sprintf("commit %x", sum),
	}
	b, err := kv.GetCommit(db, sum)
	require.NoError(t, err)
	_, c, err := objects.ReadCommitFrom(bytes.NewReader(b))
	require.NoError(t, err)
	lines = append(lines, SdumpTable(t, db, c.Table, 2))
	return strings.Join(lines, "\n")
}

func LogCommit(t *testing.T, db kvcommon.DB, msg string, sum []byte) {
	t.Logf("%s:\n%s", msg, SdumpCommit(t, db, sum))
}
