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

func TestRemoteRemoveCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()

	// add remote
	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", "https://my-repo.com"})
	require.NoError(t, cmd.Execute())

	// add some refs under remote origin
	names := []string{"branch-1", "branch-2"}
	sums := [][]byte{
		testutils.SecureRandomBytes(16),
		testutils.SecureRandomBytes(16),
	}
	for i, name := range names {
		err := ref.SaveRemoteRef(rs, "origin", name, sums[i], "test", "test@domain.com", "test", "test remote remove")
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// rename remote
	cmd.SetArgs([]string{"remote", "remove", "origin"})
	require.NoError(t, cmd.Execute())
	cmd.SetArgs([]string{"remote", "-v"})
	assertCmdOutput(t, cmd, "")

	// assert refs are updated
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	m, err := ref.ListRemoteRefs(rs, "origin")
	require.NoError(t, err)
	assert.Len(t, m, 0)
}
