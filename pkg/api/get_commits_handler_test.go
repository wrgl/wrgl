package api_test

import (
	"container/list"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/api/payload"
	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/ref"
)

func setParentCommits(com *payload.Commit, m map[string]*payload.Commit) *payload.Commit {
	com.ParentCommits = m
	return com
}

func assertCommitTreeEqual(t *testing.T, com1, com2 *payload.Commit) {
	t.Helper()
	q := list.New()
	q.PushFront([]*payload.Commit{com1, com2})
	for q.Len() > 0 {
		sl := q.Remove(q.Front()).([]*payload.Commit)
		assert.Equal(t, sl[0].AuthorName, sl[1].AuthorName)
		assert.Equal(t, sl[0].AuthorEmail, sl[1].AuthorEmail)
		assert.Equal(t, sl[0].Message, sl[1].Message)
		assert.Equal(t, sl[0].Table, sl[1].Table)
		assert.Equal(t, sl[0].Time.Format(time.RFC3339), sl[1].Time.Format(time.RFC3339))
		assert.Equal(t, sl[0].Parents, sl[1].Parents)
		for k, com := range sl[0].ParentCommits {
			q.PushFront([]*payload.Commit{com, sl[1].ParentCommits[k]})
		}
	}
}

func (s *testSuite) TestGetCommits(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, com1 := factory.CommitRandom(t, db, nil)
	sum2, com2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, com3 := factory.CommitRandom(t, db, nil)
	sum4, com4 := factory.CommitRandom(t, db, [][]byte{sum2, sum3})
	sum5, com5 := factory.CommitRandom(t, db, [][]byte{sum3})
	sum6, com6 := factory.CommitRandom(t, db, [][]byte{sum4})
	sum7, com7 := factory.CommitRandom(t, db, [][]byte{sum5, sum6})
	require.NoError(t, ref.CommitHead(rs, "main", sum7, com7))

	_, err := cli.GetCommits("heads/beta", 0)
	assert.Error(t, err)

	gcr, err := cli.GetCommits("heads/main", 0)
	require.NoError(t, err)
	assertCommitTreeEqual(t, apiserver.CommitPayload(com7), &gcr.Root)

	gcr, err = cli.GetCommits("heads/main", 1)
	require.NoError(t, err)
	assert.Equal(t, sum7, (*gcr.Sum)[:])
	assertCommitTreeEqual(t,
		setParentCommits(apiserver.CommitPayload(com7), map[string]*payload.Commit{
			hex.EncodeToString(sum5): apiserver.CommitPayload(com5),
			hex.EncodeToString(sum6): apiserver.CommitPayload(com6),
		}),
		&gcr.Root,
	)

	gcr, err = cli.GetCommits(hex.EncodeToString(sum4), 2)
	require.NoError(t, err)
	assert.Equal(t, sum4, (*gcr.Sum)[:])
	assertCommitTreeEqual(t,
		setParentCommits(apiserver.CommitPayload(com4), map[string]*payload.Commit{
			hex.EncodeToString(sum2): setParentCommits(apiserver.CommitPayload(com2), map[string]*payload.Commit{
				hex.EncodeToString(sum1): apiserver.CommitPayload(com1),
			}),
			hex.EncodeToString(sum3): apiserver.CommitPayload(com3),
		}),
		&gcr.Root,
	)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "123")
		gcr, err = cli.GetCommits("heads/main", 1, apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, gcr)
	})
	assert.Equal(t, "123", req.Header.Get("Custom-Header"))
}
