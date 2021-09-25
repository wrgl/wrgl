// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestInitCmd(t *testing.T) {
	rootDir, err := ioutil.TempDir("", "test_wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)
	_, err = os.Stat(rootDir)
	require.NoError(t, err)
	wrglDir := filepath.Join(rootDir, ".wrgl")
	viper.Set("wrgl_dir", wrglDir)
	cmd := RootCmd()
	cmd.SetArgs([]string{"init"})
	cmd.SetOut(io.Discard)
	err = cmd.Execute()
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(wrglDir, "files"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(wrglDir, "kv"))
	require.NoError(t, err)
}
