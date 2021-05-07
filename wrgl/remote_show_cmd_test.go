package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func TestRemoteShowCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	remote := "origin"
	err = versioning.SaveRemoteRef(db, remote, "my-branch", testutils.SecureRandomBytes(16))
	require.NoError(t, err)
	err = versioning.SaveRemoteRef(db, remote, "another-branch", testutils.SecureRandomBytes(16))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// add remote
	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", remote, "https://my-repo.com", "-t", "my-branch"})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "show", remote})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"* origin",
		"  URL: https://my-repo.com",
		"  Remote branches:",
		"    another-branch",
		"    my-branch      tracked",
		"",
	}, "\n"))
}
