package main

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/wrgl/utils"
)

func TestAuthAddUserCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := newRootCmd()
	email1 := testutils.RandomEmail()
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPassword(context.Background(), password1)
	cmd.SetArgs([]string{"auth", "adduser", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = newRootCmd()
	email2 := testutils.RandomEmail()
	password2 := testutils.BrokenRandomAlphaNumericString(10)
	ctx = utils.SetPassword(context.Background(), password2)
	cmd.SetArgs([]string{"auth", "adduser", email2})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listuser"})
	emails := []string{email1, email2}
	sort.Strings(emails)
	assertCmdOutput(t, cmd, strings.Join(append(emails, ""), "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "removeuser", email1})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listuser"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		email2,
		"",
	}, "\n"))
}
