// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func createCSVFile(t *testing.T, content []string) (filePath string) {
	t.Helper()
	file, err := testutils.TempFile("", "test_commit_*.csv")
	require.NoError(t, err)
	defer file.Close()
	for _, line := range content {
		_, err := fmt.Fprintln(file, line)
		require.NoError(t, err)
	}
	return file.Name()
}

func commitFile(t *testing.T, branchName, filePath, primaryKey string, args ...string) {
	t.Helper()
	cmd := RootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetArgs(append([]string{"commit", branchName, filePath, "commit message", "--primary-key", primaryKey, "-n", "1"}, args...))
	require.NoError(t, cmd.Execute())
}

func TestDiffCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)
	commitFile(t, "my-branch", fp1, "a")

	fp2 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"4,s,d",
	})
	defer os.Remove(fp2)
	commitFile(t, "my-branch", fp2, "a")

	cmd := RootCmd()
	cmd.SetArgs([]string{"diff", "my-branch", "my-branch^", "--no-gui"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	pat := regexp.MustCompile(`DIFF_(.+)_(.+)\.csv`)
	submatch := pat.FindStringSubmatch(buf.String())
	defer os.Remove(submatch[0])
	b, err := ioutil.ReadFile(submatch[0])
	require.NoError(t, err)
	assert.Equal(t, strings.Join([]string{
		fmt.Sprintf("COLUMNS IN my-branch^ (%s),a,b,c", submatch[2]),
		fmt.Sprintf("COLUMNS IN my-branch (%s),a,b,c", submatch[1]),
		fmt.Sprintf("PRIMARY KEY IN my-branch^ (%s),true,,", submatch[2]),
		fmt.Sprintf("PRIMARY KEY IN my-branch (%s),true,,", submatch[1]),
		fmt.Sprintf("BASE ROW FROM my-branch^ (%s),1,q,w", submatch[2]),
		fmt.Sprintf("MODIFIED IN my-branch (%s),1,q,e", submatch[1]),
		fmt.Sprintf("ADDED IN my-branch (%s),4,s,d", submatch[1]),
		fmt.Sprintf("REMOVED IN my-branch (%s),3,z,x", submatch[1]),
		"",
	}, "\n"), string(b))

	cmd = RootCmd()
	cmd.SetArgs([]string{"diff", "my-branch", "--no-gui"})
	buf.Reset()
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	submatch = pat.FindStringSubmatch(buf.String())
	defer os.Remove(submatch[0])
	b, err = ioutil.ReadFile(submatch[0])
	require.NoError(t, err)
	assert.Equal(t, strings.Join([]string{
		fmt.Sprintf("COLUMNS IN (%s),a,b,c", submatch[2]),
		fmt.Sprintf("COLUMNS IN my-branch (%s),a,b,c", submatch[1]),
		fmt.Sprintf("PRIMARY KEY IN (%s),true,,", submatch[2]),
		fmt.Sprintf("PRIMARY KEY IN my-branch (%s),true,,", submatch[1]),
		fmt.Sprintf("BASE ROW FROM (%s),1,q,w", submatch[2]),
		fmt.Sprintf("MODIFIED IN my-branch (%s),1,q,e", submatch[1]),
		fmt.Sprintf("ADDED IN my-branch (%s),4,s,d", submatch[1]),
		fmt.Sprintf("REMOVED IN my-branch (%s),3,z,x", submatch[1]),
		"",
	}, "\n"), string(b))
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
		"a,c,b",
		"1,e,q",
		"2,s,a",
		"4,d,s",
	})
	defer os.Remove(fp2)

	cmd := RootCmd()
	cmd.SetArgs([]string{"diff", fp2, fp1, "--no-gui", "--primary-key", "a"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	pat := regexp.MustCompile(`DIFF_(.+)_(.+)\.csv`)
	submatch := pat.FindStringSubmatch(buf.String())
	defer os.Remove(submatch[0])
	b, err := ioutil.ReadFile(submatch[0])
	require.NoError(t, err)
	com1 := fmt.Sprintf("%s (%s)", path.Base(fp2), submatch[1])
	com2 := fmt.Sprintf("%s (%s)", path.Base(fp1), submatch[2])
	assert.Equal(t, strings.Join([]string{
		fmt.Sprintf("COLUMNS IN %s,a,b,c", com2),
		fmt.Sprintf("COLUMNS IN %s,a,c,b", com1),
		fmt.Sprintf("PRIMARY KEY IN %s,true,,", com2),
		fmt.Sprintf("PRIMARY KEY IN %s,true,,", com1),
		fmt.Sprintf("BASE ROW FROM %s,1,w,q", com2),
		fmt.Sprintf("MODIFIED IN %s,1,e,q", com1),
		fmt.Sprintf("BASE ROW FROM %s,2,s,a", com2),
		fmt.Sprintf("MODIFIED IN %s,2,s,a", com1),
		fmt.Sprintf("ADDED IN %s,4,d,s", com1),
		fmt.Sprintf("REMOVED IN %s,3,x,z", com1),
		"",
	}, "\n"), string(b))
}
