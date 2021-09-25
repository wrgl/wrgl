// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
)

func TestCommitCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)

	cmd := RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1"})
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())

	rs := rd.OpenRefStore()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sl, err := objects.GetAllCommitKeys(db)
	require.NoError(t, err)
	require.Len(t, sl, 1)
	com, err := objects.GetCommit(db, sl[0])
	require.NoError(t, err)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@domain.com", com.AuthorEmail)
	assert.Equal(t, "initial commit", com.Message)
	sum, err := ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	assert.Equal(t, sl[0], sum)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/my-branch", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "commit",
		Message:     "initial commit",
	})
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"export", "my-branch"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}

func TestCommitFromStdin(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)

	f, err := os.Open(fp)
	require.NoError(t, err)
	cmd := RootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", "-", "initial commit", "-n", "1"})
	cmd.SetIn(f)
	cmd.SetOut(ioutil.Discard)
	require.NoError(t, cmd.Execute())
	require.NoError(t, f.Close())

	cmd.SetArgs([]string{"export", "my-branch"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))
}
