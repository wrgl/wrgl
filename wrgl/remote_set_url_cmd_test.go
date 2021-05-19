// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteSetURLCmd(t *testing.T) {
	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	// add remote
	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())

	// set url with bad url
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "set-url", "origin", "https//other-repo.com"})
	assert.Error(t, cmd.Execute())

	// set url
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "set-url", "origin", "https://other-repo.com"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "get-url", "origin"})
	assertCmdOutput(t, cmd, "https://other-repo.com\n")
}
