// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	localhelpers "github.com/wrgl/core/pkg/local/helpers"
)

func TestConfigAddCmd(t *testing.T) {
	cleanup := localhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "add", "user.name", "john"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("command only support multiple values field. Use \"config set\" command instead"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/heads/main"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\n")

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/tags/december"})
	require.NoError(t, cmd.Execute())

	// vanilla get & get-all
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/tags/december\n")
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\nrefs/tags/december\n")

	// get with value pattern
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "^refs/heads/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "^refs/tags/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")

	// get with fixed value
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "refs/heads/main", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "refs/tags/december", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")
}
