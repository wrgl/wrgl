// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package factory

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func Commit(t *testing.T, db objects.Store, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
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
	comSum, err := objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	c.Sum = comSum
	return comSum, c
}

func CommitRandom(t *testing.T, db objects.Store, parents [][]byte) ([]byte, *objects.Commit) {
	return Commit(t, db, nil, nil, parents)
}

func CommitHead(t *testing.T, db objects.Store, rs ref.Store, branch string, rows []string, pk []uint32) ([]byte, *objects.Commit) {
	t.Helper()
	var parents [][]byte
	commitSum, err := ref.GetHead(rs, branch)
	if err == nil {
		parents = append(parents, commitSum)
	}
	sum, c := Commit(t, db, rows, pk, parents)
	require.NoError(t, ref.CommitHead(rs, branch, sum, c))
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
