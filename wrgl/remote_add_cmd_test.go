package main

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/versioning"
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
	c, err := versioning.OpenConfig(false, viper.GetString("wrgl_dir"))
	require.NoError(t, err)
	assert.Equal(t, map[string]*versioning.ConfigRemote{
		"acme": {
			URL: "https://acme.com",
			Fetch: []*versioning.Refspec{
				versioning.MustRefspec("+refs/heads/*:refs/remotes/acme/*"),
				versioning.MustRefspec("tag *"),
			},
		},
		"beta": {
			URL: "https://beta.com",
			Fetch: []*versioning.Refspec{
				versioning.MustRefspec("+refs/heads/main:refs/remotes/beta/main"),
				versioning.MustRefspec("+refs/heads/tickets:refs/remotes/beta/tickets"),
			},
		},
		"gamma": {
			URL: "https://gamma.com",
			Fetch: []*versioning.Refspec{
				versioning.MustRefspec("+refs/*:refs/*"),
			},
		},
		"theta": {
			URL: "https://theta.com",
			Fetch: []*versioning.Refspec{
				versioning.MustRefspec("+refs/heads/*:refs/remotes/theta/*"),
			},
			Mirror: true,
		},
		"origin": {
			URL: "https://my-repo.com",
			Fetch: []*versioning.Refspec{
				versioning.MustRefspec("+refs/heads/*:refs/remotes/origin/*"),
			},
		},
	}, c.Remote)
}
