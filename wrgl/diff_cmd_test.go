package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func createCSVFile(t *testing.T, content []string) (filePath string) {
	t.Helper()
	file, err := ioutil.TempFile("", "test_commit_*.csv")
	require.NoError(t, err)
	defer file.Close()
	for _, line := range content {
		_, err := fmt.Fprintln(file, line)
		require.NoError(t, err)
	}
	return file.Name()
}

func commitFile(t *testing.T, rd *repoDir, configFilePath, branchName, filePath, primaryKey string, args ...string) {
	t.Helper()
	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	setCmdArgs(cmd, rd, configFilePath, append([]string{"commit", branchName, filePath, "commit message", "--primary-key", primaryKey, "-n", "1"}, args...)...)
	require.NoError(t, cmd.Execute())
}

func TestDiffCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	cf, cleanup := createConfigFile(t)
	defer cleanup()

	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)
	commitFile(t, rd, cf, "my-branch", fp1, "a")

	fp2 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"4,s,d",
	})
	defer os.Remove(fp2)
	commitFile(t, rd, cf, "my-branch", fp2, "a")

	cmd := newRootCmd()
	setCmdArgs(cmd, rd, cf, "diff", "my-branch", "my-branch^", "--format", "json")
	assertCmdOutput(t, cmd, strings.Join([]string{
		`{"t":1,"oldCols":["a","b","c"],"cols":["a","b","c"],"pk":["a"]}`,
		`{"t":7,"rowChangeColumns":[{"name":"a","movedFrom":-1},{"name":"b","movedFrom":-1},{"name":"c","movedFrom":-1}]}`,
		`{"t":4,"rowChangeRow":[["1"],["q"],["e","w"]]}`,
		`{"t":5,"row":["4","s","d"]}`,
		`{"t":6,"row":["3","z","x"]}`,
		``,
	}, "\n"))
}

func TestDiffCmdNoRepoDir(t *testing.T) {
	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)

	fp2 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"4,s,d",
	})
	defer os.Remove(fp2)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"diff", fp2, fp1, "--format", "json", "--primary-key", "a"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		`{"t":1,"oldCols":["a","b","c"],"cols":["a","b","c"],"pk":["a"]}`,
		`{"t":7,"rowChangeColumns":[{"name":"a","movedFrom":-1},{"name":"b","movedFrom":-1},{"name":"c","movedFrom":-1}]}`,
		`{"t":4,"rowChangeRow":[["1"],["q"],["e","w"]]}`,
		`{"t":5,"row":["4","s","d"]}`,
		`{"t":6,"row":["3","z","x"]}`,
		``,
	}, "\n"))
}
