package versioning

import (
	"sort"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

func TestCommitSave(t *testing.T) {
	s := kv.NewMockStore(false)
	tp, err := ptypes.TimestampProto(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	c1 := &objects.Commit{
		Author:        &objects.Author{Email: "author@domain.com", Name: "Author"},
		Message:       "yolo!",
		TableSum:      []byte("456qwe"),
		PrevCommitSum: []byte("asdfgh"),
		Timestamp:     tp,
	}
	hash, err := SaveCommit(s, 0, c1)
	require.NoError(t, err)
	c2, err := GetCommit(s, hash)
	require.NoError(t, err)
	assert.True(t, cmp.Equal(c1, c2, protocmp.Transform()))

	err = DeleteCommit(s, hash)
	require.NoError(t, err)

	_, err = GetCommit(s, hash)
	assert.Equal(t, kv.KeyNotFoundError, err)
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
			Author:        &objects.Author{Email: "author-1@domain.com", Name: "Author 1"},
			Message:       "msg-1",
			TableSum:      []byte("content-1"),
			PrevCommitSum: []byte("commit-1"),
		},
		{
			Author:        &objects.Author{Email: "author-2@domain.com", Name: "Author 2"},
			Message:       "msg-2",
			TableSum:      []byte("content-2"),
			PrevCommitSum: []byte("commit-2"),
		},
	}
	for _, c := range commits {
		_, err := SaveCommit(s, 0, c)
		require.NoError(t, err)
	}

	sl, err := GetAllCommits(s)
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return string(sl[i].TableSum) < string(sl[j].TableSum) })
	assertCommitsSliceEqual(t, commits, sl)

	hashes, err := GetAllCommitHashes(s)
	require.NoError(t, err)
	sl = []*objects.Commit{}
	for _, h := range hashes {
		c, err := GetCommit(s, h)
		require.NoError(t, err)
		sl = append(sl, c)
	}
	sort.Slice(sl, func(i, j int) bool { return string(sl[i].TableSum) < string(sl[j].TableSum) })
	assertCommitsSliceEqual(t, commits, sl)

	err = DeleteAllCommit(s)
	require.NoError(t, err)
	sl, err = GetAllCommits(s)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
}
