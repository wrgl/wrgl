package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigCmd(t *testing.T) {
	wrglDir, err := ioutil.TempDir("", ".wrgl*")
	require.NoError(t, err)
	defer os.RemoveAll(wrglDir)
	viper.Set("wrgl_dir", wrglDir)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "user.name", "John Doe"})
	require.NoError(t, cmd.Execute())

	cmd.SetArgs([]string{"config", "user.name"})
	assertCmdOutput(t, cmd, "John Doe\n")
}
