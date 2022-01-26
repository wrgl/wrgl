package wrgl

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestAuthSetRoleCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := rootCmd()
	email1 := testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	cmd.SetArgs([]string{"auth", "add-user", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "set-role", email1, "editor"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		auth.ScopeRepoReadConfig,
		auth.ScopeRepoWrite,
		auth.ScopeRepoWriteConfig,
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "set-role", email1, "viewer"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "set-role", "anyone", "viewer"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", "anyone"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		"",
	}, "\n"))
}
