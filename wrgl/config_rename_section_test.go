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

func TestConfigRenameSectionCmd(t *testing.T) {
	cleanup := versioning.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	for _, s := range []string{
		"refs/heads/alpha",
		"refs/tags/jan",
	} {
		cmd := newRootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", s})
		require.NoError(t, cmd.Execute())
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "rename-section", "remote.origin.push", "receive"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`types are different: []*versioning.Refspec != *versioning.ConfigReceive`))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "rename-section", "remote.origin.push", "remote.acme.fetch"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.push", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "remote.origin.push" is not set`))
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.acme.fetch", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/alpha",
		"refs/tags/jan",
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "rename-section", "remote.acme", "remote.origin"})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.acme.fetch", "--local"})
	assertCmdFailed(t, cmd, "", fmt.Errorf(`key "remote.acme.fetch" is not set`))
	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get-all", "remote.origin.fetch", "--local"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"refs/heads/alpha",
		"refs/tags/jan",
		"",
	}, "\n"))
}
