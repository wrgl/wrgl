// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"fmt"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestConfigAddCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := testutils.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "add", "user.name", "john"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("command only support multiple values field. Use \"config set\" command instead"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/heads/main"})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\n")

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "add", "remote.origin.push", "refs/tags/december"})
	require.NoError(t, cmd.Execute())

	// vanilla get
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push.0"})
	assertCmdOutput(t, cmd, "refs/heads/main\n")
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push.1"})
	assertCmdOutput(t, cmd, "refs/tags/december\n")
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, "refs/heads/main\nrefs/tags/december\n")

	// get with value pattern
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "^refs/heads/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "^refs/tags/.+", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")

	// get with fixed value
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "refs/heads/main", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/heads/main\x00")
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "refs/tags/december", "--fixed-value", "--null"})
	assertCmdOutput(t, cmd, "refs/tags/december\x00")

	// add with json value
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "add", "branch.main.primaryKey", "mycolumn"})
	require.NoError(t, cmd.Execute())

	// get json value
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "branch.main.primaryKey.0"})
	assertCmdOutput(t, cmd, "mycolumn\n")
}
