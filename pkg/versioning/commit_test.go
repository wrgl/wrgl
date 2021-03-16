package versioning

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wrgl/core/pkg/kv"
)

func TestCommitSave(t *testing.T) {
	s := kv.NewMockStore(false)
	c1 := &Commit{
		Author:         &Author{Email: "author@domain.com", Name: "Author"},
		Message:        "yolo!",
		ContentHash:    "456qwe",
		PrevCommitHash: "asdfgh",
		Timestamp:      time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	hash, err := c1.Save(s, 0)
	require.NoError(t, err)
	c2, err := GetCommit(s, hash)
	require.NoError(t, err)
	assert.Equal(t, c1, c2)

	err = DeleteCommit(s, hash)
	require.NoError(t, err)

	_, err = GetCommit(s, hash)
	assert.Equal(t, kv.KeyNotFoundError, err)
}

func TestGetAllCommits(t *testing.T) {
	s := kv.NewMockStore(false)
	commits := []*Commit{
		{
			Author:         &Author{Email: "author-1@domain.com", Name: "Author 1"},
			Message:        "msg-1",
			ContentHash:    "content-1",
			PrevCommitHash: "commit-1",
		},
		{
			Author:         &Author{Email: "author-2@domain.com", Name: "Author 2"},
			Message:        "msg-2",
			ContentHash:    "content-2",
			PrevCommitHash: "commit-2",
		},
	}
	for _, c := range commits {
		_, err := c.Save(s, 0)
		require.NoError(t, err)
	}

	sl, err := GetAllCommits(s)
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return sl[i].ContentHash < sl[j].ContentHash })
	assert.Equal(t, commits, sl)

	hashes, err := GetAllCommitHashes(s)
	require.NoError(t, err)
	sl = []*Commit{}
	for _, h := range hashes {
		c, err := GetCommit(s, h)
		require.NoError(t, err)
		sl = append(sl, c)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i].ContentHash < sl[j].ContentHash })
	assert.Equal(t, commits, sl)

	err = DeleteAllCommit(s)
	require.NoError(t, err)
	sl, err = GetAllCommits(s)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
}
