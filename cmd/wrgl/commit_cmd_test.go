// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
)

func TestCommitCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)

	cmd := RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	rs := rd.OpenRefStore()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sl, err := objects.GetAllCommitKeys(db)
	require.NoError(t, err)
	require.Len(t, sl, 1)
	com, err := objects.GetCommit(db, sl[0])
	require.NoError(t, err)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@domain.com", com.AuthorEmail)
	assert.Equal(t, "initial commit", com.Message)
	sum, err := ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	assert.Equal(t, sl[0], sum)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/my-branch", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "commit",
		Message:     "initial commit",
	})
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"export", "my-branch"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}

func TestCommitFromStdin(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)

	f, err := os.Open(fp)
	require.NoError(t, err)
	cmd := RootCmd()
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

func TestCommitSetFile(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)
	cmd := RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "second commit", "-n", "1"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("no configuration found for branch \"my-branch\". You need to specify CSV_FILE_PATH"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1", "-p", "a", "--set-file", "--set-primary-key"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	// append a single line to fp
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.Write([]string{"4", "r", "t"}))
	w.Flush()
	require.NoError(t, f.Close())

	// commit reading file and pk from config
	cmd = RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "second commit", "-n", "1"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	// check that second commit is correct
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, err := ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	com, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, tbl.PrimaryKey())
	assert.Equal(t, uint32(4), tbl.RowsCount)
	require.NoError(t, db.Close())

	// refuse to commit again because file hasn't changed
	cmd = RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "third commit", "-n", "1"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	assert.True(t, strings.HasSuffix(buf.String(), fmt.Sprintf("file %s hasn't changed since the last commit. Aborting.\n", fp)))

	// commit overriding pk
	cmd = RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "second commit", "-n", "1", "-p", "b"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	// check that pk is different
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	rs = rd.OpenRefStore()
	sum, err = ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	com, err = objects.GetCommit(db, sum)
	require.NoError(t, err)
	tbl, err = objects.GetTable(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, tbl.PrimaryKey())
}
