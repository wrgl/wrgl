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
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func rootCmd() *cobra.Command {
	cmd := RootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	return cmd
}

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

func commitFile(t *testing.T, branchName, filePath, primaryKey string, extraArgs ...string) {
	t.Helper()
	cmd := rootCmd()
	args := []string{"commit", branchName}
	if filePath != "" {
		args = append(args, filePath)
	}
	args = append(args, "commit message")
	if primaryKey != "" {
		args = append(args, "--primary-key", primaryKey)
	}
	args = append(args, "-n", "1")
	cmd.SetArgs(append(args, extraArgs...))
	require.NoError(t, cmd.Execute())
}

func assertDiffCSVEqual(t *testing.T, args []string, cb func(sum1, sum2 string) string) {
	t.Helper()
	cmd := rootCmd()
	cmd.SetArgs(append(append([]string{"diff"}, args...), "--no-gui"))
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	pat := regexp.MustCompile(`DIFF_(.+)_(.+)\.csv`)
	submatch := pat.FindStringSubmatch(buf.String())
	defer os.Remove(submatch[0])
	b, err := ioutil.ReadFile(submatch[0])
	require.NoError(t, err)
	assert.Equal(t, cb(submatch[1], submatch[2]), string(b))
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

	assertDiffCSVEqual(t, []string{"my-branch", "my-branch^"}, func(sum1, sum2 string) string {
		return strings.Join([]string{
			fmt.Sprintf("COLUMNS IN my-branch^ (%s),a,b,c", sum2),
			fmt.Sprintf("COLUMNS IN my-branch (%s),a,b,c", sum1),
			fmt.Sprintf("PRIMARY KEY IN my-branch^ (%s),true,,", sum2),
			fmt.Sprintf("PRIMARY KEY IN my-branch (%s),true,,", sum1),
			fmt.Sprintf("BASE ROW FROM my-branch^ (%s),1,q,w", sum2),
			fmt.Sprintf("MODIFIED IN my-branch (%s),1,q,e", sum1),
			fmt.Sprintf("ADDED IN my-branch (%s),4,s,d", sum1),
			fmt.Sprintf("REMOVED IN my-branch (%s),3,z,x", sum1),
			"",
		}, "\n")
	})

	assertDiffCSVEqual(t, []string{"my-branch"}, func(sum1, sum2 string) string {
		return strings.Join([]string{
			fmt.Sprintf("COLUMNS IN (%s),a,b,c", sum2),
			fmt.Sprintf("COLUMNS IN my-branch (%s),a,b,c", sum1),
			fmt.Sprintf("PRIMARY KEY IN (%s),true,,", sum2),
			fmt.Sprintf("PRIMARY KEY IN my-branch (%s),true,,", sum1),
			fmt.Sprintf("BASE ROW FROM (%s),1,q,w", sum2),
			fmt.Sprintf("MODIFIED IN my-branch (%s),1,q,e", sum1),
			fmt.Sprintf("ADDED IN my-branch (%s),4,s,d", sum1),
			fmt.Sprintf("REMOVED IN my-branch (%s),3,z,x", sum1),
			"",
		}, "\n")
	})
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

	assertDiffCSVEqual(t, []string{fp2, fp1, "--primary-key", "a"}, func(sum1, sum2 string) string {
		com1 := fmt.Sprintf("%s (%s)", path.Base(fp2), sum1)
		com2 := fmt.Sprintf("%s (%s)", path.Base(fp1), sum2)
		return strings.Join([]string{
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
		}, "\n")
	})
}

func TestDiffCmdBranchFile(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)
	commitFile(t, "my-branch", fp1, "a", "--set-file", "--set-primary-key")

	appendToFile(t, fp1, "\n4,r,t")

	assertDiffCSVEqual(t, []string{"my-branch", "--branch-file"}, func(sum1, sum2 string) string {
		name := filepath.Base(fp1)
		return strings.Join([]string{
			fmt.Sprintf("COLUMNS IN my-branch (%s),a,b,c", sum2),
			fmt.Sprintf("COLUMNS IN %s (%s),a,b,c", name, sum1),
			fmt.Sprintf("PRIMARY KEY IN my-branch (%s),true,,", sum2),
			fmt.Sprintf("PRIMARY KEY IN %s (%s),true,,", name, sum1),
			fmt.Sprintf("ADDED IN %s (%s),4,r,t", name, sum1),
			"",
		}, "\n")
	})
}

func TestDiffCmdAll(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)
	commitFile(t, "branch-1", fp1, "a", "--set-file", "--set-primary-key")
	overrideCSVFile(t, fp1, []string{
		"a,b,f",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})

	fp2 := createCSVFile(t, []string{
		"a,d,e",
		"1,e,r",
		"2,d,f",
		"3,c,v",
	})
	defer os.Remove(fp2)
	commitFile(t, "branch-2", fp2, "a", "--set-file", "--set-primary-key")

	fp3 := createCSVFile(t, []string{
		"a,f,g",
		"1,t,y",
		"2,g,h",
		"3,b,n",
	})
	defer os.Remove(fp3)
	commitFile(t, "branch-3", fp3, "a")

	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "set", "branch.branch-0.file", fp3})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"diff", "--all"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	assert.Contains(t, buf.String(), "\x1b[1mbranch-0\x1b[0m changes:\n")
	assert.Contains(t, buf.String(), "Branch not found, skipping.\n")
	assert.Contains(t, buf.String(), "\x1b[1mbranch-1\x1b[0m changes:\n")
	assert.Contains(t, buf.String(), "row changes:\n  \x1b[33m3 modified rows\n")
	assert.Contains(t, buf.String(), "\x1b[1mbranch-2\x1b[0m changes:\n")
	assert.Contains(t, buf.String(), "there are no changes\n")
	assert.NotContains(t, buf.String(), "\x1b[1mbranch-3\x1b[0m changes:\n")
}
