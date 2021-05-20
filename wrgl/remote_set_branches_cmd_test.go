// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/versioning"
)

func TestRemoteSetBranches(t *testing.T) {
	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	// add remote
	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())

	// set branches
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "set-branches", "origin", "main"})
	require.NoError(t, cmd.Execute())
	c, err := versioning.OpenConfig(false, viper.GetString("wrgl_dir"))
	require.NoError(t, err)
	assert.Equal(t, []*versioning.Refspec{
		versioning.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
	}, c.Remote["origin"].Fetch)

	// set branches --add
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "set-branches", "origin", "data", "--add"})
	require.NoError(t, cmd.Execute())
	c, err = versioning.OpenConfig(false, viper.GetString("wrgl_dir"))
	require.NoError(t, err)
	assert.Equal(t, []*versioning.Refspec{
		versioning.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
		versioning.MustParseRefspec("+refs/heads/data:refs/remotes/origin/data"),
	}, c.Remote["origin"].Fetch)
}
