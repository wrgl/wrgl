package wrgl

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	authcmd "github.com/wrgl/core/cmd/wrgl/auth"
	"github.com/wrgl/core/cmd/wrgl/utils"
	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/testutils"
)

func TestAuthAddScopeCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := RootCmd()
	email1 := testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	cmd.SetArgs([]string{"auth", "adduser", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = RootCmd()
	email := testutils.RandomEmail()
	cmd.SetArgs([]string{"auth", "addscope", email, auth.ScopeRepoRead})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "addscope", email1, "abcd"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("abcd"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "addscope", email1, auth.ScopeRepoRead, auth.ScopeRepoWrite})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		auth.ScopeRepoWrite,
		"",
	}, "\n"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email, auth.ScopeRepoWrite})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email1, "qwer"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("qwer"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "removescope", email1, auth.ScopeRepoWrite})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "listscope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		"",
	}, "\n"))
}
