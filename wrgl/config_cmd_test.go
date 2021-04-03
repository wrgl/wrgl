package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigCmd(t *testing.T) {
	file, err := ioutil.TempFile("", "test_config_*.json")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	require.NoError(t, file.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"config", "user.name", "John Doe", "--config-file", file.Name()})
	require.NoError(t, cmd.Execute())

	cmd.SetArgs([]string{"config", "user.name", "--config-file", file.Name()})
	assertCmdOutput(t, cmd, "John Doe\n")
}
