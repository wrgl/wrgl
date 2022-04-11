// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
)

func TestCredAuthCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	defer ts.Close()
	_, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()

	_, cleanUp := createRepoDir(t)
	defer cleanUp()

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "authenticate", "origin"})
	require.NoError(t, cmd.ExecuteContext(
		utils.SetPromptValues(context.Background(), []string{server_testutils.Email, server_testutils.Password}),
	))

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "list"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		url,
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "remove", url})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "list"})
	assertCmdOutput(t, cmd, "")

	tokFile := filepath.Join(t.TempDir(), "tok.txt")
	require.NoError(t, ioutil.WriteFile(tokFile, []byte(ts.AdminToken(t)), 0644))
	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "authenticate", url, "--token-location", tokFile})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "list"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		url,
		"",
	}, "\n"))
}
