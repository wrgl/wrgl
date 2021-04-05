package versioning

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestParseNavigationChars(t *testing.T) {
	for _, c := range []struct {
		commitStr, name string
		goBack          int
	}{
		{"my-branch^", "my-branch", 1},
		{"my-branch", "my-branch", 0},
		{"my-branch^^", "my-branch", 2},
		{"my-branch~0", "my-branch", 0},
		{"my-branch~4", "my-branch", 4},
	} {
		name, goBack, err := parseNavigationChars(c.commitStr)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)
		assert.Equal(t, c.goBack, goBack)
	}
}

func TestGetPrevCommit(t *testing.T) {
	db := kv.NewMockStore(false)
	commit1 := &objects.Commit{TableSum: []byte("abc")}
	sum1, err := SaveCommit(db, 0, commit1)
	require.NoError(t, err)
	commit2 := &objects.Commit{TableSum: []byte("def"), PrevCommitSum: sum1}
	sum2, err := SaveCommit(db, 0, commit2)
	require.NoError(t, err)
	commit3 := &objects.Commit{TableSum: []byte("qwe"), PrevCommitSum: sum2}
	sum3, err := SaveCommit(db, 0, commit3)
	require.NoError(t, err)

	sum, commit, err := getPrevCommit(db, sum3, commit3, 0)
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	assert.True(t, cmp.Equal(commit3, commit, protocmp.Transform()))

	sum, commit, err = getPrevCommit(db, sum3, commit3, 1)
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	assert.True(t, cmp.Equal(commit2, commit, protocmp.Transform()))

	sum, commit, err = getPrevCommit(db, sum3, commit3, 2)
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	assert.True(t, cmp.Equal(commit1, commit, protocmp.Transform()))
}

func TestInterpretCommitName(t *testing.T) {
	db := kv.NewMockStore(false)
	commit1 := &objects.Commit{TableSum: []byte("abc")}
	sum1, err := SaveCommit(db, 0, commit1)
	require.NoError(t, err)
	commit2 := &objects.Commit{TableSum: []byte("def"), PrevCommitSum: sum1}
	sum2, err := SaveCommit(db, 0, commit2)
	require.NoError(t, err)
	branch := &objects.Branch{CommitSum: sum2}
	branchName := "my-branch"
	err = SaveBranch(db, branchName, branch)
	require.NoError(t, err)
	file, err := ioutil.TempFile("", "test_versioning_*.csv")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	require.NoError(t, file.Close())

	for i, c := range []struct {
		db        kv.DB
		commitStr string
		sum       []byte
		commit    *objects.Commit
		fileIsNil bool
		err       error
	}{
		{db, "my-branch", sum2, commit2, true, nil},
		{db, "my-branch^", sum1, commit1, true, nil},
		{db, hex.EncodeToString(sum2), sum2, commit2, true, nil},
		{db, fmt.Sprintf("%s~1", hex.EncodeToString(sum2)), sum1, commit1, true, nil},
		{db, file.Name(), nil, nil, false, nil},
		{db, "aaaabbbbccccdddd0000111122223333", nil, nil, true, fmt.Errorf("can't find commit aaaabbbbccccdddd0000111122223333")},
		{db, "some-branch", nil, nil, true, fmt.Errorf("can't find branch some-branch")},
		{db, "abc.csv", nil, nil, true, fmt.Errorf("can't find file abc.csv")},
		{nil, "my-branch", nil, nil, true, fmt.Errorf("can't find file my-branch")},
		{nil, hex.EncodeToString(sum2), nil, nil, true, fmt.Errorf("can't find file %s", hex.EncodeToString(sum2))},
		{nil, file.Name(), nil, nil, false, nil},
	} {
		sum, commit, file, err := InterpretCommitName(c.db, c.commitStr)
		require.Equal(t, c.err, err, "case %d", i)
		assert.Equal(t, c.sum, sum)
		assert.True(t, cmp.Equal(c.commit, commit, protocmp.Transform()))
		assert.Equal(t, c.fileIsNil, file == nil)
	}
}
