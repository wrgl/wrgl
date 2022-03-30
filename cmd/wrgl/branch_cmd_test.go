// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/errors"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func createRepoDir(t *testing.T) (rd *local.RepoDir, cleanup func()) {
	t.Helper()
	rootDir, err := testutils.TempDir("", "test_wrgl_*")
	require.NoError(t, err)
	wrglDir := filepath.Join(rootDir, ".wrgl")
	rd, err = local.NewRepoDir(wrglDir, "")
	require.NoError(t, err)
	err = rd.Init()
	require.NoError(t, err)
	viper.Set("wrgl_dir", wrglDir)
	cmd := rootCmd()
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
	exErr := cmd.Execute()
	assert.True(t, errors.Contains(exErr, err), "expecting error %v to contain error %v", exErr, err)
	assert.Equal(t, output, buf.String())
}

func TestBranchCmdList(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	// test list branch
	cmd.SetArgs([]string{"branch", "list"})
	assertCmdOutput(t, cmd, "alpha\nbeta\n")

	// test list branch with pattern
	cmd.SetArgs([]string{"branch", "list", "al*"})
	assertCmdOutput(t, cmd, "alpha\n")

	// test list branch with multiple patterns
	cmd.SetArgs([]string{"branch", "list", "al*", "b*"})
	assertCmdOutput(t, cmd, "alpha\nbeta\n")
}

func TestBranchCmdCopy(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, c := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "create", "delta", "--copy", "gamma"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "create", "beta", "--copy", "alpha"})
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "create", "gamma", "--copy", "alpha"})
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
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "create", "--move", "delta", "gamma"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "create", "--move", "beta", "alpha"})
	assert.Equal(t, `branch "beta" already exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "create", "--move", "gamma", "alpha"})
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
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "delete", "gamma"})
	assert.Equal(t, `branch "gamma" does not exist`, cmd.Execute().Error())

	cmd.SetArgs([]string{"branch", "delete", "alpha"})
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
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"branch", "create", "delta"})
	assert.Error(t, cmd.Execute())

	cmd.SetArgs([]string{"branch", "create", "delta", "alpha"})
	assertCmdOutput(t, cmd, fmt.Sprintf("created branch delta (%s)\n", hex.EncodeToString(sum)))

	cmd.SetArgs([]string{"branch", "create", "beta", hex.EncodeToString(sum)})
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

func TestBranchCmdConfig(t *testing.T) {
	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	branch := "my-branch"
	cmd := rootCmd()
	cmd.SetArgs([]string{"branch", "config", branch})
	assert.Equal(t, fmt.Errorf(`branch "my-branch" not found`), cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"branch", "config", branch, "--set-file", "my_data.csv", "--set-primary-key", "id", "--set-delimiter", "|"})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"branch", "config", branch})
	assertCmdOutput(t, cmd, strings.Join([]string{
		`{`,
		`    "file": "my_data.csv",`,
		`    "primaryKey": [`,
		`        "id"`,
		`    ],`,
		`    "delimiter": 124`,
		`}`,
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"branch", "config", branch, "--set-upstream-remote", "origin", "--set-upstream-dest", "refs/heads/my-other-branch"})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"branch", "config", branch})
	assertCmdOutput(t, cmd, strings.Join([]string{
		`{`,
		`    "remote": "origin",`,
		`    "merge": "refs/heads/my-other-branch",`,
		`    "file": "my_data.csv",`,
		`    "primaryKey": [`,
		`        "id"`,
		`    ],`,
		`    "delimiter": 124`,
		`}`,
		"",
	}, "\n"))
}
