// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestConfigUnsetCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := testutils.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	// test unset string
	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "john"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "user.name"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "user.name" is not set`))

	for _, s := range []string{
		"refs/heads/alpha",
		"refs/heads/beta",
	} {
		cmd := rootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	// test unset with value pattern
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "remote.origin.push", "refs/tags"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "remote.origin.push", "^refs/.+"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("key contains multiple values, specify flag --all to remove multiple values"))
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "remote.origin.push", "refs/heads/alpha", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "--local"})
	assertCmdOutput(t, cmd, "refs/heads/beta\n")
}

func TestConfigUnsetAllCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := testutils.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	// test unset string
	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "john"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "user.name", "--all"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
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
		cmd := rootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "--all", "remote.origin.push", "refs/heads/alpha", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/beta",
		"refs/heads/gamma",
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "--all", "remote.origin.push", "^refs/tags/.+"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/beta",
		"refs/heads/gamma",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "unset", "--all", "remote.origin.push"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "remote.origin.push" is not set`))
}
