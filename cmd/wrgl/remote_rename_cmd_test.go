// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestRemoteRenameCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()

	// add remote
	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())

	// add some refs under remote origin
	names := []string{"branch-1", "branch-2"}
	sums := [][]byte{
		testutils.SecureRandomBytes(16),
		testutils.SecureRandomBytes(16),
	}
	for i, name := range names {
		err := ref.SaveRemoteRef(rs, "origin", name, sums[i], "test", "test@domain.com", "test", "test remote rename")
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// rename remote
	cmd.SetArgs([]string{"remote", "rename", "origin", "acme"})
	require.NoError(t, cmd.Execute())
	cmd.SetArgs([]string{"remote", "-v"})
	assertCmdOutput(t, cmd, "acme https://my-repo.com\n")

	// assert refs are updated
	m, err := ref.ListRemoteRefs(rs, "origin")
	require.NoError(t, err)
	assert.Len(t, m, 0)
	m, err = ref.ListRemoteRefs(rs, "acme")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)
}
