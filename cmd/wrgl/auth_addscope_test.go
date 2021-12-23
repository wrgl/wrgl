package wrgl

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	authcmd "github.com/wrgl/wrgl/cmd/wrgl/auth"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func logAuthnContent(t *testing.T, rd *local.RepoDir) {
	t.Helper()
	f, err := os.Open(filepath.Join(rd.FullPath, "authn.csv"))
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	t.Logf("data from authn.csv:\n%s", string(b))
	require.NoError(t, f.Close())
}

func TestAuthAddScopeCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := rootCmd()
	email1 := testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	cmd.SetArgs([]string{"auth", "add-user", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))
	logAuthnContent(t, rd)

	cmd = rootCmd()
	email := testutils.RandomEmail()
	cmd.SetArgs([]string{"auth", "add-scope", email, auth.ScopeRepoRead})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "add-scope", email1, "abcd"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("abcd"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "add-scope", email1, "--all"})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", email})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

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
	cmd.SetArgs([]string{"auth", "remove-scope", email, auth.ScopeRepoWrite})
	assertCmdFailed(t, cmd, "", authcmd.UserNotFoundErr(email))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "remove-scope", email1, "qwer"})
	assertCmdFailed(t, cmd, "", authcmd.InvalidScopeErr("qwer"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "remove-scope", email1, auth.ScopeRepoWrite})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", email1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		auth.ScopeRepoReadConfig,
		auth.ScopeRepoWriteConfig,
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "add-scope", auth.Anyone, auth.ScopeRepoRead})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", auth.Anyone})
	assertCmdOutput(t, cmd, strings.Join([]string{
		auth.ScopeRepoRead,
		"",
	}, "\n"))
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "remove-scope", auth.Anyone, auth.ScopeRepoRead})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"auth", "list-scope", auth.Anyone})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"",
	}, "\n"))
}
