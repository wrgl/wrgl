// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/wrgl/utils"
)

func TestRemoteAddCmd(t *testing.T) {
	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	// add remote
	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "acme", "https://acme.com", "--tags"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "beta", "https://beta.com", "-t", "main", "-t", "tickets"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "gamma", "https://gamma.com", "--mirror=fetch"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "theta", "https://theta.com", "--mirror=push"})
	require.NoError(t, cmd.Execute())

	// list remote non-verbose
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"acme", "beta", "gamma", "origin", "theta", "",
	}, "\n"))

	// list remote verbose
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "--verbose"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"acme https://acme.com",
		"beta https://beta.com",
		"gamma https://gamma.com",
		"origin https://my-repo.com",
		"theta https://theta.com",
		"",
	}, "\n"))

	// test config
	c, err := utils.OpenConfig(false, false, viper.GetString("wrgl_dir"), "")
	require.NoError(t, err)
	assert.Equal(t, map[string]*conf.ConfigRemote{
		"acme": {
			URL: "https://acme.com",
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/*:refs/remotes/acme/*"),
				conf.MustParseRefspec("tag *"),
			},
		},
		"beta": {
			URL: "https://beta.com",
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/main:refs/remotes/beta/main"),
				conf.MustParseRefspec("+refs/heads/tickets:refs/remotes/beta/tickets"),
			},
		},
		"gamma": {
			URL: "https://gamma.com",
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/*:refs/*"),
			},
		},
		"theta": {
			URL: "https://theta.com",
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/*:refs/remotes/theta/*"),
			},
			Mirror: true,
		},
		"origin": {
			URL: "https://my-repo.com",
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
			},
		},
	}, c.Remote)

	// test get-url
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "get-url", "origin"})
	assertCmdOutput(t, cmd, "https://my-repo.com\n")
}
