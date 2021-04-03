package main

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
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
	setCmdArgs(cmd, rd, "commit", "my-branch", fp, "initial commit", "-n", "1")
	cmd.SetErr(ioutil.Discard)
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	setCmdArgs(cmd, rd, "export", "my-branch")
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}

func TestCommitCmdBigTable(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createRandomCSVFile(t)
	defer os.Remove(fp)

	cmd := newRootCmd()
	setCmdArgs(cmd, rd, "commit", "my-branch", fp, "initial commit", "-n", "1", "--big-table")
	cmd.SetErr(ioutil.Discard)
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	setCmdArgs(cmd, rd, "export", "my-branch")
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}
