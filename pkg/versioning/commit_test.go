package versioning

import (
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func TestCommitSave(t *testing.T) {
	s := kv.NewMockStore(false)
	c1 := &objects.Commit{
		AuthorEmail: "author@domain.com",
		AuthorName:  "Author",
		Message:     "yolo!",
		Table:       testutils.SecureRandomBytes(16),
		Parents:     [][]byte{testutils.SecureRandomBytes(16)},
		Time:        time.Now(),
	}
	hash, err := SaveCommit(s, 0, c1)
	require.NoError(t, err)
	c2, err := GetCommit(s, hash)
	require.NoError(t, err)
	assert.True(t, CommitExist(s, hash))
	objects.AssertCommitEqual(t, c1, c2)

	err = DeleteCommit(s, hash)
	require.NoError(t, err)

	_, err = GetCommit(s, hash)
	assert.Equal(t, kv.KeyNotFoundError, err)
	assert.False(t, CommitExist(s, hash))
}

func assertCommitsSliceEqual(t *testing.T, sl1, sl2 []*objects.Commit) {
	t.Helper()
	assert.Equal(t, len(sl1), len(sl2))
	for i, c := range sl1 {
		assert.True(t, cmp.Equal(c, sl2[i], protocmp.Transform()))
	}
}

func TestGetAllCommits(t *testing.T) {
	s := kv.NewMockStore(false)
	commits := []*objects.Commit{
		{
			AuthorEmail: "author-1@domain.com",
			AuthorName:  "Author 1",
			Message:     "msg-1",
			Table:       testutils.SecureRandomBytes(16),
			Parents:     [][]byte{testutils.SecureRandomBytes(16)},
			Time:        time.Now().Round(time.Second),
		},
		{
			AuthorEmail: "author-2@domain.com",
			AuthorName:  "Author 2",
			Message:     "msg-2",
			Table:       testutils.SecureRandomBytes(16),
			Parents:     [][]byte{testutils.SecureRandomBytes(16)},
			Time:        time.Now().Add(24 * time.Hour).Round(time.Second),
		},
	}
	sort.Slice(commits, func(i, j int) bool { return string(commits[i].Table[:]) < string(commits[j].Table[:]) })
	for _, c := range commits {
		_, err := SaveCommit(s, 0, c)
		require.NoError(t, err)
	}

	sl, err := GetAllCommits(s)
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return string(sl[i].Table[:]) < string(sl[j].Table[:]) })
	assertCommitsSliceEqual(t, commits, sl)

	hashes, err := GetAllCommitHashes(s)
	require.NoError(t, err)
	sl = []*objects.Commit{}
	for _, h := range hashes {
		c, err := GetCommit(s, h)
		require.NoError(t, err)
		sl = append(sl, c)
	}
	sort.Slice(sl, func(i, j int) bool { return string(sl[i].Table[:]) < string(sl[j].Table[:]) })
	assertCommitsSliceEqual(t, commits, sl)

	err = DeleteAllCommit(s)
	require.NoError(t, err)
	sl, err = GetAllCommits(s)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
}
