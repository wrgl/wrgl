// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestInitCmd(t *testing.T) {
	dir, cleanup := testutils.ChTempDir(t)
	defer cleanup()
	wrglDir := filepath.Join(dir, ".wrgl")
	viper.Set("wrgl_dir", "")
	cmd := rootCmd()
	cmd.SetArgs([]string{"init"})
	require.NoError(t, cmd.Execute())
	assert.DirExists(t, filepath.Join(wrglDir, "kv"))
}

func TestInitCmdDirExists(t *testing.T) {
	dir, err := testutils.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	viper.Set("wrgl_dir", "")
	cmd := rootCmd()
	cmd.SetArgs([]string{"init", "--wrgl-dir", dir})
	require.NoError(t, cmd.Execute())
	assert.DirExists(t, filepath.Join(dir, "kv"))
}
