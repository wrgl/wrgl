// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
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

	_, fp := createCSVFile(t, []string{
		"a|b|c",
		"1|q|w",
		"2|a|s",
		"3|z|x",
	})
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "my-other-branch", fp, "initial commit", "-n", "1", "--delimiter", "|"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	_, fp = createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	rs := rd.OpenRefStore()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	com, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@domain.com", com.AuthorEmail)
	assert.Equal(t, "initial commit", com.Message)
	sum2, err := ref.GetHead(rs, "my-other-branch")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
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

	_, fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)

	f, err := os.Open(fp)
	require.NoError(t, err)
	cmd := rootCmd()
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

func overrideFile(t *testing.T, fp string, cb func(*os.File)) {
	t.Helper()
	f, err := os.Create(fp)
	require.NoError(t, err)
	cb(f)
	require.NoError(t, f.Close())
}

func overrideCSVFile(t *testing.T, fp string, rows []string) {
	t.Helper()
	overrideFile(t, fp, func(f *os.File) {
		w := csv.NewWriter(f)
		defer w.Flush()
		rowStrs := make([][]string, 0, len(rows))
		for _, row := range rows {
			rowStrs = append(rowStrs, strings.Split(row, ","))
		}
		require.NoError(t, w.WriteAll(rowStrs))
	})
}

func appendToFile(t *testing.T, fp string, content string) {
	t.Helper()
	fi, err := os.Stat(fp)
	require.NoError(t, err)
	f, err := os.OpenFile(fp, os.O_RDWR, fi.Mode())
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestCommitSetFile(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	_, fp := createCSVFile(t, []string{
		"a|b|c",
		"1|q|w",
		"2|a|s",
		"3|z|x",
	})
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "second commit", "-n", "1"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("branch.file is not set for branch \"my-branch\". You need to specify CSV_FILE_PATH"))

	cmd = rootCmd()
	cmd.SetArgs([]string{
		"commit", "my-branch", fp, "initial commit",
		"-n", "1",
		"-p", "a",
		"--delimiter", "|",
		"--set-file",
		"--set-primary-key",
	})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	// append a single line to fp
	appendToFile(t, fp, "\n4|r|t")

	// commit reading file and pk from config
	cmd = rootCmd()
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
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "third commit", "-n", "1"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	assert.True(t, strings.HasSuffix(buf.String(), fmt.Sprintf("file %s hasn't changed since the last commit. Aborting.\n", fp)))

	// commit overriding pk
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "third commit", "-n", "1", "-p", "b"})
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

func TestCommitCmdAll(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	rs := rd.OpenRefStore()

	_, fp1 := createCSVFile(t, []string{
		"a|b|c",
		"1|q|w",
		"2|a|s",
		"3|z|x",
	})
	defer os.Remove(fp1)
	commitFile(t, "branch-1", fp1, "a", "--set-file", "--set-primary-key", "--delimiter", "|")
	sum1, err := ref.GetHead(rs, "branch-1")
	require.NoError(t, err)

	_, fp2 := createCSVFile(t, []string{
		"a,d,e",
		"1,e,r",
		"2,d,f",
		"3,c,v",
	})
	defer os.Remove(fp2)
	commitFile(t, "branch-2", fp2, "a", "--set-file", "--set-primary-key")
	sum2, err := ref.GetHead(rs, "branch-2")
	require.NoError(t, err)

	_, fp3 := createCSVFile(t, []string{
		"a,f,g",
		"1,t,y",
		"2,g,h",
		"3,b,n",
	})
	defer os.Remove(fp3)
	commitFile(t, "branch-3", fp3, "a")
	sum3, err := ref.GetHead(rs, "branch-3")
	require.NoError(t, err)

	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "set", "branch.branch-4.file", "non-existent.csv"})
	require.NoError(t, cmd.Execute())

	appendToFile(t, fp1, "\n4|t|y")

	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "--all", "mass commit"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	assert.Contains(t, buf.String(), "branch \"branch-2\" is up-to-date.\n")
	assert.Contains(t, buf.String(), `File "non-existent.csv" does not exist, skipping branch "branch-4".`)

	sum, err := ref.GetHead(rs, "branch-2")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	sum, err = ref.GetHead(rs, "branch-3")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	sum, err = ref.GetHead(rs, "branch-1")
	require.NoError(t, err)
	assert.NotEqual(t, sum1, sum)
	assert.Contains(t, buf.String(), fmt.Sprintf("[branch-1 %s] mass commit\n", hex.EncodeToString(sum)[:7]))

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	com, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, "mass commit", com.Message)
}
