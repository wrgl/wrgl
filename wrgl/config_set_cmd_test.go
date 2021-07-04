// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	wrglhelpers "github.com/wrgl/core/wrgl/helpers"
)

func TestConfigSetCmd(t *testing.T) {
	cleanup := wrglhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "John Doe"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "John Smith", "--global"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "set", "user.name", "Jane Lane", "--system"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name", "--local"})
	assertCmdOutput(t, cmd, "John Doe\n")

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name", "--system"})
	assertCmdOutput(t, cmd, "Jane Lane\n")

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name", "--global"})
	assertCmdOutput(t, cmd, "John Smith\n")

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "user.name"})
	assertCmdOutput(t, cmd, "John Doe\n")
}

func TestConfigSetCmdBool(t *testing.T) {
	cleanup := wrglhelpers.MockGlobalConf(t, true)
	defer cleanup()
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "set", "receive.denyDeletes", "true"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "set", "receive.denyNonFastForwards", "false"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "receive.denyDeletes"})
	assertCmdOutput(t, cmd, "true\n")

	cmd = newRootCmd()
	cmd.SetArgs([]string{"config", "get", "receive.denyNonFastForwards"})
	assertCmdOutput(t, cmd, "false\n")
}
