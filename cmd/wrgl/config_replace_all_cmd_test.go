// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/core/pkg/conf/helpers"
)

func TestConfigReplaceAllCmd(t *testing.T) {
	cleanup := confhelpers.MockGlobalConf(t, true)
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
		cmd := RootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	cmd := RootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/heads/theta", "^refs/heads/"})
	require.NoError(t, cmd.Execute())
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/jan",
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"",
	}, "\n"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/apr", "refs/tags/jan", "--fixed-value"})
	require.NoError(t, cmd.Execute())
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/feb",
		"refs/tags/mar",
		"refs/heads/theta",
		"refs/tags/apr",
		"",
	}, "\n"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "replace-all", "remote.origin.push", "refs/tags/may"})
	require.NoError(t, cmd.Execute())
	cmd = RootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/tags/may",
		"",
	}, "\n"))
}
