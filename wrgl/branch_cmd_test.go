package main

import (
	"bytes"
	"encoding/hex"
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

func createRepoDir(t *testing.T) (rd *versioning.RepoDir, cleanup func()) {
	t.Helper()
	rootDir, err := ioutil.TempDir("", "test_wrgl_*")
	require.NoError(t, err)
	rd = versioning.NewRepoDir(rootDir, false, false)
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

func setCmdArgs(cmd *cobra.Command, rd *versioning.RepoDir, configFilePath string, args ...string) {
	cmd.SetArgs(append(args, "--root-dir", rd.RootDir, "--config-file", configFilePath))
}

func TestBranchCmdList(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	factory.Commit(t, db, fs, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	// test list branch
	setCmdArgs(cmd, rd, cf, "branch")
	assertCmdOutput(t, cmd, "alpha\nbeta\n")

	// test list branch with pattern
	setCmdArgs(cmd, rd, cf, "branch", "--list", "al*")
	assertCmdOutput(t, cmd, "alpha\n")

	// test list branch with multiple patterns
	setCmdArgs(cmd, rd, cf, "branch", "--list", "al*", "--list", "b*")
	assertCmdOutput(t, cmd, "alpha\nbeta\n")
}

func TestBranchCmdCopy(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum, _ := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	factory.Commit(t, db, fs, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, cf, "branch", "gamma", "--copy", "delta")
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, cf, "branch", "alpha", "--copy", "beta")
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, cf, "branch", "alpha", "--copy", "gamma")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetHead(db, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	b2, err := versioning.GetHead(db, "alpha")
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
}

func TestBranchCmdMove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum, _ := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	factory.Commit(t, db, fs, "beta", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, cf, "branch", "gamma", "--move", "delta")
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, cf, "branch", "alpha", "--move", "beta")
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	setCmdArgs(cmd, rd, cf, "branch", "alpha", "--move", "gamma")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetHead(db, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	_, err = versioning.GetHead(db, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdDelete(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, cf, "branch", "gamma", "--delete")
	assertCmdOutput(t, cmd, "deleted branch gamma\n")

	setCmdArgs(cmd, rd, cf, "branch", "alpha", "--delete")
	assertCmdOutput(t, cmd, "deleted branch alpha\n")

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	_, err = versioning.GetHead(db, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdCreate(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum, _ := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, cf, "branch", "delta")
	assert.Equal(t, "please specify both branch name and start point (could be branch name, commit hash)", cmd.Execute().Error())

	setCmdArgs(cmd, rd, cf, "branch", "delta", "alpha")
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch delta (%s)\n", hex.EncodeToString(sum)))

	setCmdArgs(cmd, rd, cf, "branch", "beta", hex.EncodeToString(sum))
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch beta (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := versioning.GetHead(db, "delta")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	b2, err := versioning.GetHead(db, "beta")
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
}
