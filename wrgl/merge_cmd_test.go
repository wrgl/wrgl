package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/versioning"
)

func TestMergeCmdCommitCSV(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	base, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
	}, []uint32{0})
	sum1, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,e",
		"2,a,d",
	}, []uint32{0})
	sum2, com2 := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, [][]byte{base})
	require.NoError(t, versioning.CommitHead(db, fs, "branch-2", sum2, com2))
	require.NoError(t, db.Close())

	fp := createCSVFile(t, []string{
		"a,b,c",
		"1,q,e",
		"2,a,d",
		"3,z,x",
	})
	defer os.Remove(fp)
	cmd := newRootCmd()
	cmd.SetArgs([]string{"merge", "branch-1", "branch-2", "--commit-csv", fp})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"export", "branch-1"})
	b, err := ioutil.ReadFile(fp)
	require.NoError(t, err)
	assertCmdOutput(t, cmd, string(b))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetHead(db, "branch-1")
	require.NoError(t, err)
	versioning.AssertLatestReflogEqual(t, fs, "heads/branch-1", &objects.Reflog{
		OldOID:      sum1,
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "merge",
		Message:     fmt.Sprintf("merge %s, %s", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7]),
	})
	com, err := versioning.GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum1, sum2}, com.Parents)
	assert.Equal(t, "Merge \"branch-2\" into \"branch-1\"", com.Message)
}
