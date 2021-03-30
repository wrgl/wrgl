package versioning

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
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
	commit1 := &Commit{ContentHash: "abc"}
	sum1, err := commit1.Save(db, 0)
	require.NoError(t, err)
	commit2 := &Commit{ContentHash: "def", PrevCommitHash: sum1}
	sum2, err := commit2.Save(db, 0)
	require.NoError(t, err)
	commit3 := &Commit{ContentHash: "qwe", PrevCommitHash: sum2}
	sum3, err := commit3.Save(db, 0)
	require.NoError(t, err)

	sum, commit, err := getPrevCommit(db, sum3, commit3, 0)
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	assert.Equal(t, commit3, commit)

	sum, commit, err = getPrevCommit(db, sum3, commit3, 1)
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	assert.Equal(t, commit2, commit)

	sum, commit, err = getPrevCommit(db, sum3, commit3, 2)
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	assert.Equal(t, commit1, commit)
}

func TestInterpretCommitName(t *testing.T) {
	db := kv.NewMockStore(false)
	commit1 := &Commit{ContentHash: "abc"}
	sum1, err := commit1.Save(db, 0)
	require.NoError(t, err)
	commit2 := &Commit{ContentHash: "def", PrevCommitHash: sum1}
	sum2, err := commit2.Save(db, 0)
	require.NoError(t, err)
	branch := &Branch{CommitHash: sum2}
	branchName := "my-branch"
	err = branch.Save(db, branchName)
	require.NoError(t, err)
	file, err := ioutil.TempFile("", "test_versioning_*.csv")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	require.NoError(t, file.Close())

	for _, c := range []struct {
		commitStr, sum string
		commit         *Commit
		fileIsNil      bool
		err            error
	}{
		{"my-branch", sum2, commit2, true, nil},
		{"my-branch^", sum1, commit1, true, nil},
		{sum2, sum2, commit2, true, nil},
		{fmt.Sprintf("%s~1", sum2), sum1, commit1, true, nil},
		{file.Name(), "", nil, false, nil},
		{"aaaabbbbccccdddd0000111122223333", "", nil, true, fmt.Errorf("can't find commit aaaabbbbccccdddd0000111122223333")},
		{"some-branch", "", nil, true, fmt.Errorf("can't find branch some-branch")},
		{"abc.csv", "", nil, true, fmt.Errorf("can't find file abc.csv")},
	} {
		sum, commit, file, err := InterpretCommitName(db, c.commitStr)
		require.Equal(t, c.err, err)
		assert.Equal(t, c.sum, sum)
		assert.Equal(t, c.commit, commit)
		assert.Equal(t, c.fileIsNil, file == nil)
	}
}
