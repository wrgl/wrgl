package wrgl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/cmd/wrgl/utils"
	"github.com/wrgl/core/pkg/testutils"
)

func TestAuthAddUserCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	cmd := RootCmd()
	email1 := "a" + testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	cmd.SetArgs([]string{"auth", "add-user", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = RootCmd()
	email2 := "b" + testutils.RandomEmail()
	name2 := testutils.BrokenRandomLowerAlphaString(10)
	password2 := testutils.BrokenRandomAlphaNumericString(10)
	ctx = utils.SetPromptValues(context.Background(), []string{name2, password2})
	cmd.SetArgs([]string{"auth", "add-user", email2})
	require.NoError(t, cmd.ExecuteContext(ctx))

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
