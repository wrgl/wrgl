// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/versioning"
)

func TestConfigUnsetAllCmd(t *testing.T) {
	cleanup := versioning.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	// test unset string
	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "john"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "unset-all", "user.name"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "user.name" is not set`))

	for _, s := range []string{
		"refs/heads/alpha",
		"refs/heads/beta",
		"refs/heads/gamma",
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
	} {
		cmd := newRootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "unset-all", "remote.origin.push", "refs/heads/alpha", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/beta",
		"refs/heads/gamma",
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "unset-all", "remote.origin.push", "^refs/tags/.+"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/beta",
		"refs/heads/gamma",
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "unset-all", "remote.origin.push"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "remote.origin.push" is not set`))
}
