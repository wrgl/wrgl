package main

import (
	"context"
	"fmt"
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
	email1 := "a" + testutils.RandomEmail()
	name1 := testutils.BrokenRandomLowerAlphaString(10)
	password1 := testutils.BrokenRandomAlphaNumericString(10)
	ctx := utils.SetPromptValues(context.Background(), []string{name1, password1})
	cmd.SetArgs([]string{"auth", "adduser", email1})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = newRootCmd()
	email2 := "b" + testutils.RandomEmail()
	name2 := testutils.BrokenRandomLowerAlphaString(10)
	password2 := testutils.BrokenRandomAlphaNumericString(10)
	ctx = utils.SetPromptValues(context.Background(), []string{name2, password2})
	cmd.SetArgs([]string{"auth", "adduser", email2})
	require.NoError(t, cmd.ExecuteContext(ctx))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listuser"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s <%s>", name1, email1),
		fmt.Sprintf("%s <%s>", name2, email2),
		"",
	}, "\n"))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "removeuser", email1})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"auth", "listuser"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s <%s>", name2, email2),
		"",
	}, "\n"))
}
