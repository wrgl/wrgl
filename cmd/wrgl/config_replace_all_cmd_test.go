// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestConfigReplaceAllCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := testutils.TempDir("", ".wrgl*")
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
		cmd := rootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/heads/theta", "^refs/heads/"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/apr", "refs/tags/jan", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"refs/tags/apr",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/may"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "get", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/may",
		"",
	}, "\n"))
}
