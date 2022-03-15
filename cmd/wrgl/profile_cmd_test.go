// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func TestProfileCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	// create first commit
	_, fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "initial commit", "-n", "1"})
	require.NoError(t, cmd.Execute())

	// remove table profile of first commit
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, err := ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	com, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	tblProf1, err := objects.GetTableProfile(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), tblProf1.RowsCount)
	require.NoError(t, objects.DeleteTableProfile(db, com.Table))
	require.NoError(t, db.Close())

	// refresh table profile of first commit
	cmd = rootCmd()
	cmd.SetArgs([]string{"profile", "my-branch", "--refresh", "--silent"})
	require.NoError(t, cmd.Execute())

	// check that table profile is created
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	tblProf2, err := objects.GetTableProfile(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, tblProf1, tblProf2)
	require.NoError(t, db.Close())

	// create second commit
	_, fp = createCSVFile(t, []string{
		"a,b,c",
		"1,w,w",
		"2,a,f",
		"3,x,n",
	})
	defer os.Remove(fp)
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "my-branch", fp, "second commit", "-n", "1"})
	require.NoError(t, cmd.Execute())

	// remove table profiles of all commits
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	require.NoError(t, objects.DeleteTableProfile(db, com.Table))
	sum, err = ref.GetHead(rs, "my-branch")
	require.NoError(t, err)
	com2, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	require.NoError(t, objects.DeleteTableProfile(db, com2.Table))
	require.NoError(t, db.Close())

	// refresh table profile of first commit
	cmd = rootCmd()
	cmd.SetArgs([]string{"profile", "my-branch", "--refresh", "--ancestors"})
	require.NoError(t, cmd.Execute())

	// check that table profile is created
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	_, err = objects.GetTableProfile(db, com.Table)
	require.NoError(t, err)
	_, err = objects.GetTableProfile(db, com2.Table)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
