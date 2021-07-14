// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func TestUploadPack(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, c1 := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, _ := factory.CommitRandom(t, db, nil)
	sum4, _ := factory.CommitRandom(t, db, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))
	require.NoError(t, ref.SaveTag(rs, "v1", sum4))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", NewUploadPackHandler(db, rs, 0))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	commits := packtest.FetchObjects(t, dbc, rsc, [][]byte{sum2}, 0)
	assert.Equal(t, [][]byte{sum1, sum2}, commits)
	packtest.AssertCommitsPersisted(t, db, commits)

	packtest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "main", sum1, c1))
	c, err := packclient.NewClient(packtest.TestOrigin)
	require.NoError(t, err)
	_, err = packclient.NewUploadPackSession(db, rs, c, [][]byte{sum2}, 0)
	assert.Error(t, err, "nothing wanted")

	packtest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum3})
	require.NoError(t, ref.SaveTag(rsc, "v0", sum3))
	commits = packtest.FetchObjects(t, dbc, rsc, [][]byte{sum2, sum4}, 1)
	packtest.AssertCommitsPersisted(t, db, commits)
}

func TestUploadPackMultiplePackfiles(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := packtest.CreateRandomCommit(t, db, 5, 700, nil)
	sum2, _ := packtest.CreateRandomCommit(t, db, 5, 700, [][]byte{sum1})
	sum3, c3 := packtest.CreateRandomCommit(t, db, 5, 700, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rs, "main", sum3, c3))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", NewUploadPackHandler(db, rs, 1024))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	commits := packtest.FetchObjects(t, dbc, rsc, [][]byte{sum3}, 0)
	assert.Equal(t, [][]byte{sum1, sum2, sum3}, commits)
	packtest.AssertCommitsPersisted(t, db, commits)
}
