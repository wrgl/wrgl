package main

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func TestMergeCmdCommitCSV(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
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
	ts, err := table.ReadTable(db, fs, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())
}

func TestMergeCmdCommitCSVCustomMessage(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
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
	cmd.SetArgs([]string{"merge", "branch-1", "branch-2", "--commit-csv", fp, "-m", "my merge message", "-p", "b"})
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
	assert.Equal(t, "my merge message", com.Message)
	ts, err := table.ReadTable(db, fs, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, ts.PrimaryKey())
}

func TestMergeCmdNoGUI(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	fs := rd.OpenFileStore()
	base, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,v,b",
	}, []uint32{0})
	sum1, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,d",
		"1,g,e",
		"2,h,d",
	}, []uint32{0})
	sum2, com2 := factory.Commit(t, db, fs, []string{
		"a,c,e",
		"1,q,w",
		"3,z,x",
	}, []uint32{0}, [][]byte{base})
	require.NoError(t, versioning.CommitHead(db, fs, "branch-2", sum2, com2))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"merge", "branch-1", "branch-2", "--no-gui"})
	name := fmt.Sprintf("CONFLICTS_%s_%s.csv", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7])
	assertCmdOutput(t, cmd, fmt.Sprintf("saved conflicts to file %s\n", name))

	defer os.Remove(name)
	f, err := os.Open(name)
	require.NoError(t, err)
	defer f.Close()
	rows := []string{}
	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rows = append(rows, strings.Join(row, ","))
	}
	remInCom2 := strings.Repeat(fmt.Sprintf(",REMOVED IN %s", hex.EncodeToString(sum2)[:7]), 5)
	assert.Equal(t, []string{
		",a,c,e,b,d",
		fmt.Sprintf("COLUMNS: branch-1 (%s),,REMOVED,,,NEW", hex.EncodeToString(sum1)[:7]),
		fmt.Sprintf("COLUMNS: branch-2 (%s),,,NEW,REMOVED,", hex.EncodeToString(sum2)[:7]),
		fmt.Sprintf("BASE %s,1,w,,q,", hex.EncodeToString(base)[:7]),
		fmt.Sprintf("branch-1 (%s),1,e,,g,", hex.EncodeToString(sum1)[:7]),
		fmt.Sprintf("branch-2 (%s),1,w,,q,", hex.EncodeToString(sum2)[:7]),
		"RESOLUTION,1,w,w,q,e",
		fmt.Sprintf("BASE %s,2,s,,a,", hex.EncodeToString(base)[:7]),
		fmt.Sprintf("branch-1 (%s),2,d,,h,", hex.EncodeToString(sum1)[:7]),
		fmt.Sprintf("branch-2 (%s)%s", hex.EncodeToString(sum2)[:7], remInCom2),
		"RESOLUTION,2,,,h,d",
		",3,z,x,,",
	}, rows)
}

func readCSV(t *testing.T, filename string) ([]byte, [][]string) {
	t.Helper()
	f, err := os.Open(filename)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	reader := csv.NewReader(bytes.NewReader(b))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	return b, rows
}

func TestMergeCmdAutoResolve(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	fs := rd.OpenFileStore()
	base, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,c,d",
		"1,q,w,o",
		"2,a,s,k",
	}, []uint32{0})
	sum1, _ := factory.CommitHead(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,e",
		"2,a,d",
	}, []uint32{0})
	sum2, com2 := factory.Commit(t, db, fs, []string{
		"a,b,c,d",
		"1,q,w,o",
		"2,a,s,k",
		"3,z,x,l",
	}, []uint32{0}, [][]byte{base})
	require.NoError(t, versioning.CommitHead(db, fs, "branch-2", sum2, com2))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"merge", "branch-1", "branch-2", "--no-commit"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	name := fmt.Sprintf("MERGE_%s_%s.csv", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7])
	defer os.Remove(name)
	b, rows := readCSV(t, name)
	assert.Equal(t, [][]string{
		{"a", "b", "c"},
		{"1", "q", "e"},
		{"2", "a", "d"},
		{"3", "z", "x"},
	}, rows)

	cmd = newRootCmd()
	cmd.SetArgs([]string{"merge", "branch-1", "branch-2"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"export", "branch-1"})
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
	ts, err := table.ReadTable(db, fs, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())
}
