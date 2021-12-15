// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func TestRemoteSetBranches(t *testing.T) {
	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	// add remote
	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())

	// set branches
	cmd = rootCmd()
	cmd.SetArgs([]string{"remote", "set-branches", "origin", "main"})
	require.NoError(t, cmd.Execute())
	cs := conffs.NewStore(viper.GetString("wrgl_dir"), conffs.LocalSource, "")
	c, err := cs.Open()
	require.NoError(t, err)
	assert.Equal(t, []*conf.Refspec{
		conf.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
	}, c.Remote["origin"].Fetch)

	// set branches --add
	cmd = rootCmd()
	cmd.SetArgs([]string{"remote", "set-branches", "origin", "data", "--add"})
	require.NoError(t, cmd.Execute())
	c, err = cs.Open()
	require.NoError(t, err)
	assert.Equal(t, []*conf.Refspec{
		conf.MustParseRefspec("+refs/heads/main:refs/remotes/origin/main"),
		conf.MustParseRefspec("+refs/heads/data:refs/remotes/origin/data"),
	}, c.Remote["origin"].Fetch)
}
