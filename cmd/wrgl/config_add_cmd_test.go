// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
)

func TestConfigAddCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := RootCmd()
	cmd.SetArgs([]string{"config", "add", "user.name", "john"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("command only support multiple values field. Use \"config set\" command instead"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/heads/main"})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\n")

	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/tags/december"})
	require.NoError(t, cmd.Execute())

	// vanilla get & get-all
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/tags/december\n")
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\nrefs/tags/december\n")

	// get with value pattern
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "^refs/heads/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "^refs/tags/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")

	// get with fixed value
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "refs/heads/main", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "refs/tags/december", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")
}
