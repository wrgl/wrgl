// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/versioning"
)

func TestConfigReplaceAllCmd(t *testing.T) {
	cleanup := versioning.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

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

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/heads/theta", "^refs/heads/.+"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/apr", "refs/tags/jan", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"refs/tags/apr",
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/may"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/may",
		"",
	}, "\n"))
}
