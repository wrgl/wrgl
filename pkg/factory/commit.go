// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package factory

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func Commit(t *testing.T, db kv.DB, fs kv.FileStore, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	c := &objects.Commit{
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		Message:     testutils.BrokenRandomAlphaNumericString(20),
		Time:        time.Now(),
		Parents:     parents,
	}
	sum, _ := BuildTable(t, db, fs, rows, pk)
	c.Table = sum
	sum, err := versioning.SaveCommit(db, 0, c)
	require.NoError(t, err)
	return sum, c
}

func CommitRandom(t *testing.T, db kv.DB, fs kv.FileStore, parents [][]byte) ([]byte, *objects.Commit) {
	return Commit(t, db, fs, nil, nil, parents)
}

func CommitHead(t *testing.T, db kv.DB, fs kv.FileStore, branch string, rows []string, pk []uint32) ([]byte, *objects.Commit) {
	t.Helper()
	var parents [][]byte
	commitSum, err := versioning.GetHead(db, branch)
	if err == nil {
		parents = append(parents, commitSum)
	}
	sum, c := Commit(t, db, fs, rows, pk, parents)
	require.NoError(t, versioning.CommitHead(db, fs, branch, sum, c))
	return sum, c
}

func CommitTag(t *testing.T, db kv.DB, fs kv.FileStore, tag string, rows []string, pk []uint32, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	sum, c := Commit(t, db, fs, rows, pk, parents)
	require.NoError(t, versioning.SaveTag(db, tag, sum))
	return sum, c
}

func SdumpCommit(t *testing.T, db kv.DB, fs kv.FileStore, sum []byte) string {
	t.Helper()
	lines := []string{
		fmt.Sprintf("commit %x", sum),
	}
	c, err := versioning.GetCommit(db, sum)
	require.NoError(t, err)
	lines = append(lines, SdumpTable(t, db, fs, c.Table, 2))
	return strings.Join(lines, "\n")
}

func LogCommit(t *testing.T, db kv.DB, fs kv.FileStore, msg string, sum []byte) {
	t.Logf("%s:\n%s", msg, SdumpCommit(t, db, fs, sum))
}
