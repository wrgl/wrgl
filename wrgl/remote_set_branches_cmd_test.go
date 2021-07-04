// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/wrgl/utils"
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
	c, err := utils.OpenConfig(false, false, viper.GetString("wrgl_dir"), "")
	require.NoError(t, err)
	assert.Equal(t, []*conf.Refspec{
		conf.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
	}, c.Remote["origin"].Fetch)

	// set branches --add
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "set-branches", "origin", "data", "--add"})
	require.NoError(t, cmd.Execute())
	c, err = utils.OpenConfig(false, false, viper.GetString("wrgl_dir"), "")
	require.NoError(t, err)
	assert.Equal(t, []*conf.Refspec{
		conf.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
		conf.MustParseRefspec("+refs/heads/data:refs/remotes/origin/data"),
	}, c.Remote["origin"].Fetch)
}
