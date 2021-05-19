// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/kv"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/versioning"
)

func TestUploadPack(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, c1 := factory.CommitRandom(t, db, fs, nil)
	sum2, c2 := factory.CommitRandom(t, db, fs, [][]byte{sum1})
	sum3, _ := factory.CommitRandom(t, db, fs, nil)
	sum4, _ := factory.CommitRandom(t, db, fs, [][]byte{sum3})
	require.NoError(t, versioning.CommitHead(db, fs, "main", sum2, c2))
	require.NoError(t, versioning.SaveTag(db, "v1", sum4))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", NewUploadPackHandler(db, fs))

	dbc := kv.NewMockStore(false)
	fsc := kv.NewMockStore(false)
	oc := packtest.FetchObjects(t, dbc, fsc, [][]byte{sum2}, 0)
	packtest.AssertSentMissingCommits(t, db, fs, oc, [][]byte{sum1, sum2}, nil)

	packtest.CopyCommitsToNewStore(t, db, dbc, fs, fsc, [][]byte{sum1})
	require.NoError(t, versioning.CommitHead(dbc, fsc, "main", sum1, c1))
	oc = packtest.FetchObjects(t, dbc, fsc, [][]byte{sum2}, 0)
	packtest.AssertSentMissingCommits(t, db, fs, oc, [][]byte{sum2}, [][]byte{sum1})

	packtest.CopyCommitsToNewStore(t, db, dbc, fs, fsc, [][]byte{sum3})
	require.NoError(t, versioning.SaveTag(dbc, "v0", sum3))
	oc = packtest.FetchObjects(t, dbc, fsc, [][]byte{sum2, sum4}, 1)
	packtest.AssertSentMissingCommits(t, db, fs, oc, [][]byte{sum2, sum4}, [][]byte{sum1, sum3})
}
