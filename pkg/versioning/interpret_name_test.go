package versioning

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
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
	sum1, commit1 := SaveTestCommit(t, db, nil)
	sum2, commit2 := SaveTestCommit(t, db, [][]byte{sum1})
	sum3, commit3 := SaveTestCommit(t, db, [][]byte{sum2})

	sum, commit, err := peelCommit(db, sum3, commit3, 0)
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	objects.AssertCommitEqual(t, commit3, commit)

	sum, commit, err = peelCommit(db, sum3, commit3, 1)
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	objects.AssertCommitEqual(t, commit2, commit)

	sum, commit, err = peelCommit(db, sum3, commit3, 2)
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	objects.AssertCommitEqual(t, commit1, commit)
}

func TestInterpretCommitName(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, commit1 := SaveTestCommit(t, db, nil)
	sum2, commit2 := SaveTestCommit(t, db, [][]byte{sum1})
	branchName := "my-branch"
	err := CommitHead(db, fs, branchName, sum2, commit2)
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
		if c.commit == nil {
			assert.Nil(t, commit)
		} else {
			objects.AssertCommitEqual(t, c.commit, commit)
		}
		assert.Equal(t, c.fileIsNil, file == nil)
	}
}
