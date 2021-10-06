// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	"github.com/wrgl/wrgl/pkg/conf"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func (s *testSuite) TestUploadPack(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := apitest.CreateRandomCommit(t, db, 5, 1000, nil)
	sum2, c2 := apitest.CreateRandomCommit(t, db, 5, 1000, [][]byte{sum1})
	sum3, _ := apitest.CreateRandomCommit(t, db, 5, 1000, nil)
	sum4, _ := apitest.CreateRandomCommit(t, db, 5, 1000, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))
	require.NoError(t, ref.SaveTag(rs, "v1", sum4))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	commits := apitest.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2}, 0)
	assert.Equal(t, [][]byte{sum1, sum2}, commits)
	apitest.AssertCommitsPersisted(t, db, commits)

	apitest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "main", sum1, c1))
	_, err := apiclient.NewUploadPackSession(db, rs, cli, [][]byte{sum2}, 0)
	assert.Error(t, err, "nothing wanted")

	apitest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum3})
	require.NoError(t, ref.SaveTag(rsc, "v0", sum3))
	commits = apitest.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2, sum4}, 1)
	apitest.AssertCommitsPersisted(t, db, commits)
}

func (s *testSuite) TestUploadPackMultiplePackfiles(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	c, err := cs.Open()
	if err != nil {
		panic(err)
	}
	c.Pack = &conf.Pack{
		MaxFileSize: 1024,
	}
	sum1, _ := apitest.CreateRandomCommit(t, db, 5, 1000, nil)
	sum2, c2 := apitest.CreateRandomCommit(t, db, 5, 1000, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	commits := apitest.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2}, 0)
	assert.Equal(t, [][]byte{sum1, sum2}, commits)
	apitest.AssertCommitsPersisted(t, db, commits)
}

func (s *testSuite) TestUploadPackCustomHeader(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := apitest.CreateRandomCommit(t, db, 3, 4, nil)
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "asd")
		commits := apitest.FetchObjects(t, dbc, rsc, cli, [][]byte{sum1}, 0, apiclient.WithHeader(header))
		assert.Equal(t, [][]byte{sum1}, commits)
		apitest.AssertCommitsPersisted(t, db, commits)
	})
	assert.Equal(t, "asd", req.Header.Get("Custom-Header"))
}
