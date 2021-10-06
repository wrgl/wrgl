package wrgl

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func execPrintDebug(t *testing.T, cmd *cobra.Command, ctx context.Context, args []string) {
	t.Helper()
	f, err := testutils.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	defer os.Remove(f.Name())
	cmd.SetArgs(append(args, "--debug-file", f.Name()))
	require.NoError(t, cmd.ExecuteContext(ctx))
	b, err := ioutil.ReadFile(f.Name())
	require.NoError(t, err)
	t.Logf("debug file content:\n%s", string(b))
}

func TestAuthAddUserCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := RootCmd()
	email1 := "a" + testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	// cmd.SetArgs([]string{"auth", "add-user", email1})
	// require.NoError(t, cmd.ExecuteContext(ctx))
	execPrintDebug(t, cmd, ctx, []string{"auth", "add-user", email1})

	logAuthnContent(t, rd)
	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "list-user"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s <%s>", name1, email1),
		"",
	}, "\n"))

	cmd = RootCmd()
	email2 := "b" + testutils.RandomEmail()
	name2 := testutils.BrokenRandomLowerAlphaString(10)
	password2 := testutils.BrokenRandomAlphaNumericString(10)
	ctx = utils.SetPromptValues(context.Background(), []string{name2, password2})
	// cmd.SetArgs([]string{"auth", "add-user", email2})
	// require.NoError(t, cmd.ExecuteContext(ctx))
	execPrintDebug(t, cmd, ctx, []string{"auth", "add-user", email2})

	logAuthnContent(t, rd)
	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "list-user"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s <%s>", name1, email1),
		fmt.Sprintf("%s <%s>", name2, email2),
		"",
	}, "\n"))

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "remove-user", email1})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"auth", "list-user"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s <%s>", name2, email2),
		"",
	}, "\n"))
}
