// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
)

func createRepoDir(t *testing.T) (rd *local.RepoDir, cleanup func()) {
	t.Helper()
	rootDir, err := ioutil.TempDir("", "test_wrgl_*")
	require.NoError(t, err)
	wrglDir := filepath.Join(rootDir, ".wrgl")
	rd = local.NewRepoDir(wrglDir, false, false)
	err = rd.Init()
	require.NoError(t, err)
	viper.Set("wrgl_dir", wrglDir)
	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "set", "user.email", "john@domain.com"})
	require.NoError(t, cmd.Execute())
	cmd.SetArgs([]string{"config", "set", "user.name", "John Doe"})
	require.NoError(t, cmd.Execute())
	return rd, func() { os.RemoveAll(rootDir) }
}

func assertCmdOutput(t *testing.T, cmd *cobra.Command, output string) {
	t.Helper()
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	err := cmd.Execute()
	assert.Equal(t, output, buf.String())
	require.NoError(t, err)
}

func assertCmdFailed(t *testing.T, cmd *cobra.Command, output string, err error) {
	t.Helper()
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	assert.Equal(t, err, cmd.Execute())
	assert.Equal(t, output, buf.String())
}

func TestBranchCmdList(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	// test list branch
	cmd.SetArgs([]string{"branch"})
	assertCmdOutput(t, cmd, "alpha\nbeta\n")

	// test list branch with pattern
	cmd.SetArgs([]string{"branch", "--list", "al*"})
	assertCmdOutput(t, cmd, "alpha\n")

	// test list branch with multiple patterns
	cmd.SetArgs([]string{"branch", "--list", "al*", "--list", "b*"})
	assertCmdOutput(t, cmd, "alpha\nbeta\n")
}

func TestBranchCmdCopy(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, c := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "gamma", "--copy", "delta"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "alpha", "--copy", "beta"})
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "alpha", "--copy", "gamma"})
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := ref.GetHead(rs, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	b2, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/gamma", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  c.AuthorName,
		AuthorEmail: c.AuthorEmail,
		Action:      "commit",
		Message:     c.Message,
	})
}

func TestBranchCmdMove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "gamma", "--move", "delta"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "alpha", "--move", "beta"})
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "alpha", "--move", "gamma"})
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch gamma (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := ref.GetHead(rs, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	_, err = ref.GetHead(rs, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdDelete(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "gamma", "--delete"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "alpha", "--delete"})
	assertCmdOutput(t, cmd, "deleted branch alpha\n")

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	_, err = ref.GetHead(rs, "alpha")
	assert.Error(t, err)
}

func TestBranchCmdCreate(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := newRootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "delta"})
	assert.Equal(t, "please specify both branch name and start point (could be branch name, commit hash)", cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "delta", "alpha"})
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch delta (%s)\n", hex.EncodeToString(sum)))

	cmd.SetArgs([]string{"branch", "beta", hex.EncodeToString(sum)})
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch beta (%s)\n", hex.EncodeToString(sum)))

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	b1, err := ref.GetHead(rs, "delta")
	require.NoError(t, err)
	assert.Equal(t, sum, b1)
	b2, err := ref.GetHead(rs, "beta")
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/delta", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "branch",
		Message:     "created from alpha",
	})
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/beta", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "branch",
		Message:     "created from " + hex.EncodeToString(sum),
	})
}
