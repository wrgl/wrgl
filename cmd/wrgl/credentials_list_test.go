// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func parseURL(t *testing.T, s string) url.URL {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return *u
}

func TestCredentialsListCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	cs, err := credentials.NewStore()
	require.NoError(t, err)
	cs.SetRPT(parseURL(t, "http://repo1.com"), "abc")
	cs.SetRPT(parseURL(t, "http://repo2.com"), "def")
	require.NoError(t, cs.Flush())

	cmd := rootCmd()
	cmd.SetArgs([]string{"credentials", "list"})
	assertCmdOutput(t, cmd, "http://repo1.com\nhttp://repo2.com\n")

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "remove", "http://repo1.com", "http://repo2.com"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"Removed credentials for http://repo1.com",
		"Removed credentials for http://repo2.com",
		fmt.Sprintf("Saved changes to %s", cs.Path()),
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"credentials", "list"})
	assertCmdOutput(t, cmd, "")
}
