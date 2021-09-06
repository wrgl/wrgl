package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/testutils"
	authcmd "github.com/wrgl/core/wrgl/auth"
	"github.com/wrgl/core/wrgl/utils"
)

func TestAuthAddScopeCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := newRootCmd()
	email1 := testutils.RandomEmail()
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPassword(context.Background(), password1)
	cmd.SetArgs([]string{"auth", "adduser", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = newRootCmd()
	email := testutils.RandomEmail()
	cmd.SetArgs([]string{"auth", "addscope", email, auth.ScopeRead})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "addscope", email1, "abcd"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("abcd"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "addscope", email1, auth.ScopeRead, auth.ScopeWrite})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRead,
		auth.ScopeWrite,
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email, auth.ScopeWrite})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email1, "qwer"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("qwer"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email1, auth.ScopeWrite})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRead,
		"",
	}, "\n"))
}
