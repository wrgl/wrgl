package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/versioning"
)

func createRepoDir(t *testing.T) (*repoDir, func()) {
	t.Helper()
	rootDir, err := ioutil.TempDir("", "test_wrgl_*")
	require.NoError(t, err)
	rd := &repoDir{
		rootDir: rootDir,
	}
	err = rd.Init()
	require.NoError(t, err)
	return rd, func() { os.RemoveAll(rootDir) }
}

func assertCmdOutput(t *testing.T, cmd *cobra.Command, output string) {
	t.Helper()
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	err := cmd.Execute()
	require.NoError(t, err)
	assert.Equal(t, output, buf.String())
}

func setCmdArgs(cmd *cobra.Command, rd *repoDir, args ...string) {
	cmd.SetArgs(append(args, "--root-dir", rd.rootDir))
}

func TestBranchCmdList(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	factory.CommitSmall(t, db, "alpha", nil, nil, nil)
	factory.CommitSmall(t, db, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	// test list branch
	setCmdArgs(cmd, rd, "branch")
	assertCmdOutput(t, cmd, "alpha\nbeta\n")

	// test list branch with pattern
	setCmdArgs(cmd, rd, "branch", "--list", "al*")
	assertCmdOutput(t, cmd, "alpha\n")

	// test list branch with multiple patterns
	setCmdArgs(cmd, rd, "branch", "--list", "al*", "--list", "b*")
	assertCmdOutput(t, cmd, "alpha\nbeta\n")
}

func TestBranchCmdCopy(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	sum, _ := factory.CommitSmall(t, db, "alpha", nil, nil, nil)
	factory.CommitSmall(t, db, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, "branch", "gamma", "--copy", "delta")
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, "branch", "alpha", "--copy", "beta")
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, "branch", "alpha", "--copy", "gamma")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", sum))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetBranch(db, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1.CommitHash)
	b2, err := versioning.GetBranch(db, "alpha")
	require.NoError(t, err)
	assert.Equal(t, b1.CommitHash, b2.CommitHash)
}

func TestBranchCmdMove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	sum, _ := factory.CommitSmall(t, db, "alpha", nil, nil, nil)
	factory.CommitSmall(t, db, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, "branch", "gamma", "--move", "delta")
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, "branch", "alpha", "--move", "beta")
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, "branch", "alpha", "--move", "gamma")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", sum))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetBranch(db, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1.CommitHash)
	_, err = versioning.GetBranch(db, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdDelete(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	factory.CommitSmall(t, db, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, "branch", "gamma", "--delete")
	assertCmdOutput(t, cmd, "deleted branch gamma\n")

	setCmdArgs(cmd, rd, "branch", "alpha", "--delete")
	assertCmdOutput(t, cmd, "deleted branch alpha\n")

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	_, err = versioning.GetBranch(db, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdCreate(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	sum, _ := factory.CommitSmall(t, db, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, "branch", "delta")
	assert.Equal(t, "please specify both branch name and start point (could be branch name, commit hash)", cmd.Execute().Error())

	setCmdArgs(cmd, rd, "branch", "delta", "alpha")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch delta (%s)\n", sum))

	setCmdArgs(cmd, rd, "branch", "beta", sum)
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch beta (%s)\n", sum))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetBranch(db, "delta")
	require.NoError(t, err)
	assert.Equal(t, sum, b1.CommitHash)
	b2, err := versioning.GetBranch(db, "beta")
	require.NoError(t, err)
	assert.Equal(t, b1.CommitHash, b2.CommitHash)
}
