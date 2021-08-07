// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func TestUploadPack(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, c1 := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, _ := factory.CommitRandom(t, db, nil)
	sum4, _ := factory.CommitRandom(t, db, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))
	require.NoError(t, ref.SaveTag(rs, "v1", sum4))
	cli, cleanup := createClient(t, &apitest.GZIPAwareHandler{
		T:       t,
		Handler: apiserver.NewUploadPackHandler(db, rs, apiserver.NewUploadPackSessionMap(), 0),
	})
	defer cleanup()

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

func TestUploadPackMultiplePackfiles(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := apitest.CreateRandomCommit(t, db, 5, 700, nil)
	sum2, _ := apitest.CreateRandomCommit(t, db, 5, 700, [][]byte{sum1})
	sum3, c3 := apitest.CreateRandomCommit(t, db, 5, 700, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rs, "main", sum3, c3))
	cli, cleanup := createClient(t, &apitest.GZIPAwareHandler{
		T:       t,
		Handler: apiserver.NewUploadPackHandler(db, rs, apiserver.NewUploadPackSessionMap(), 1024),
	})
	defer cleanup()

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	commits := apitest.FetchObjects(t, dbc, rsc, cli, [][]byte{sum3}, 0)
	assert.Equal(t, [][]byte{sum1, sum2, sum3}, commits)
	apitest.AssertCommitsPersisted(t, db, commits)
}
