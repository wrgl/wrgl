// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	objmock "github.com/wrgl/core/pkg/objects/mock"
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
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", NewUploadPackHandler(db, rs))

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	oc := packtest.FetchObjects(t, dbc, rsc, [][]byte{sum2}, 0)
	packtest.AssertSentMissingCommits(t, db, oc, [][]byte{sum1, sum2}, nil)

	packtest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "main", sum1, c1))
	oc = packtest.FetchObjects(t, dbc, rsc, [][]byte{sum2}, 0)
	packtest.AssertSentMissingCommits(t, db, oc, [][]byte{sum2}, [][]byte{sum1})

	packtest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum3})
	require.NoError(t, ref.SaveTag(rsc, "v0", sum3))
	oc = packtest.FetchObjects(t, dbc, rsc, [][]byte{sum2, sum4}, 1)
	packtest.AssertSentMissingCommits(t, db, oc, [][]byte{sum2, sum4}, [][]byte{sum1, sum3})
}
