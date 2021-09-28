// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
)

func TestRemoteShowCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	remote := "origin"
	err = ref.SaveRemoteRef(rs, remote, "my-branch", testutils.SecureRandomBytes(16), "test", "test@domain.com", "test", "test remote show")
	require.NoError(t, err)
	err = ref.SaveRemoteRef(rs, remote, "another-branch", testutils.SecureRandomBytes(16), "test", "test@domain.com", "test", "test remote show")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// add remote
	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", remote, "https://my-repo.com", "-t", "my-branch"})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"remote", "show", remote})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"* origin",
		"  URL: https://my-repo.com",
		"  Fetch:",
		"    +refs/heads/my-branch:refs/remotes/origin/my-branch",
		"  Remote branches:",
		"    another-branch",
		"    my-branch      tracked",
		"",
	}, "\n"))
}
