// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func createRandomCSVFile(t *testing.T) (filePath string) {
	t.Helper()
	file, err := ioutil.TempFile("", "test_commit_*.csv")
	require.NoError(t, err)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	for i := 0; i < 4; i++ {
		record := []string{}
		for j := 0; j < 3; j++ {
			record = append(record, testutils.BrokenRandomLowerAlphaString(3))
		}
		require.NoError(t, writer.Write(record))
	}
	return file.Name()
}

func TestCommitCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createRandomCSVFile(t)
	defer os.Remove(fp)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	cmd.SetArgs([]string{"export", "my-branch"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))

	fs := rd.OpenFileStore()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetHead(db, "my-branch")
	require.NoError(t, err)
	versioning.AssertLatestReflogEqual(t, fs, "heads/my-branch", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "commit",
		Message:     "initial commit",
	})
}

func TestCommitFromStdin(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createRandomCSVFile(t)
	defer os.Remove(fp)

	f, err := os.Open(fp)
	require.NoError(t, err)
	cmd := newRootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "-", "initial commit", "-n", "1"})
	cmd.SetIn(f)
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())
	require.NoError(t, f.Close())

	cmd.SetArgs([]string{"export", "my-branch"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}
