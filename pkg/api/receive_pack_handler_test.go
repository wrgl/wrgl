// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	apiserver "github.com/wrgl/core/pkg/api/server"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func assertRefEqual(t *testing.T, rs ref.Store, r string, sum []byte) {
	t.Helper()
	b, err := ref.GetRef(rs, r)
	if sum == nil {
		assert.Empty(t, b)
	} else {
		require.NoError(t, err)
		assert.Equal(t, sum, b)
	}
}

func TestReceivePackHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	sum3, _ := factory.CommitHead(t, db, rs, "delta", nil, nil)
	sum7, _ := factory.CommitHead(t, db, rs, "theta", nil, nil)
	apitest.RegisterHandler(
		http.MethodPost, api.PathReceivePack, apiserver.NewReceivePackHandler(db, rs, apitest.ReceivePackConfig(false, false), apiserver.NewReceivePackSessionMap()),
	)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	apitest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1, sum2, sum7})
	sum4, c4 := factory.CommitRandom(t, dbc, [][]byte{sum1})
	sum5, c5 := factory.CommitRandom(t, dbc, nil)
	sum9, _ := factory.CommitRandom(t, dbc, nil)
	sum6, c6 := factory.CommitRandom(t, dbc, [][]byte{sum9})
	sum8, c8 := factory.CommitRandom(t, dbc, nil)
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum4, c4))
	require.NoError(t, ref.CommitHead(rsc, "gamma", sum5, c5))
	require.NoError(t, ref.CommitHead(rsc, "delta", sum6, c6))
	require.NoError(t, ref.CommitHead(rsc, "theta", sum8, c8))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum1), Sum: payload.BytesToHex(sum4)}, // fast-forward
		"refs/heads/beta":  {OldSum: payload.BytesToHex(sum2)},                                // delete
		"refs/heads/gamma": {Sum: payload.BytesToHex(sum5)},                                   // create
		"refs/heads/delta": {OldSum: payload.BytesToHex(sum9), Sum: payload.BytesToHex(sum6)}, // outdated ref
		"refs/heads/theta": {OldSum: payload.BytesToHex(sum7), Sum: payload.BytesToHex(sum8)}, // non-fast-forward
	}
	updates = apitest.PushObjects(t, dbc, rsc, updates, remoteRefs, 0)
	assert.Equal(t, "remote ref updated since checkout", updates["refs/heads/delta"].ErrMsg)
	delete(updates, "refs/heads/delta")
	updates = apitest.PushObjects(t, dbc, rsc, updates, remoteRefs, 0)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	assert.Empty(t, updates["refs/heads/beta"].ErrMsg)
	assert.Empty(t, updates["refs/heads/gamma"].ErrMsg)
	assert.Empty(t, updates["refs/heads/theta"].ErrMsg)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1, sum4, sum5})
	assertRefEqual(t, rs, "heads/alpha", sum4)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		OldOID:      sum1,
		NewOID:      sum4,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "update ref",
	})
	assertRefEqual(t, rs, "heads/beta", nil)
	assertRefEqual(t, rs, "heads/gamma", sum5)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/gamma", &ref.Reflog{
		NewOID:      sum5,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "create ref",
	})
	assertRefEqual(t, rs, "heads/delta", sum3)
	assertRefEqual(t, rs, "heads/theta", sum8)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/theta", &ref.Reflog{
		OldOID:      sum7,
		NewOID:      sum8,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "update ref",
	})

	// delete only
	updates = map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum4)}, // fast-forward
	}
	updates = apitest.PushObjects(t, dbc, rsc, updates, remoteRefs, 0)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	_, err = ref.GetHead(rs, "alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	assert.True(t, objects.CommitExist(db, sum4))
}

func TestReceivePackHandlerNoDeletesNoFastForwards(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	apitest.RegisterHandler(
		http.MethodPost, api.PathReceivePack, apiserver.NewReceivePackHandler(db, rs, apitest.ReceivePackConfig(true, true), apiserver.NewReceivePackSessionMap()),
	)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	sum3, c3 := factory.CommitRandom(t, dbc, nil)
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum3, c3))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum1), Sum: payload.BytesToHex(sum3)},
		"refs/heads/beta":  {OldSum: payload.BytesToHex(sum2)},
	}
	updates = apitest.PushObjects(t, dbc, rsc, updates, remoteRefs, 0)
	assert.Equal(t, "remote does not support non-fast-fowards", updates["refs/heads/alpha"].ErrMsg)
	assert.Equal(t, "remote does not support deleting refs", updates["refs/heads/beta"].ErrMsg)
	assertRefEqual(t, rs, "heads/alpha", sum1)
	assertRefEqual(t, rs, "heads/beta", sum2)
}

func TestReceivePackHandlerMultiplePackfiles(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	apitest.RegisterHandler(
		http.MethodPost, api.PathReceivePack, apiserver.NewReceivePackHandler(db, rs, apitest.ReceivePackConfig(true, true), apiserver.NewReceivePackSessionMap()),
	)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc := refmock.NewStore()
	sum1, _ := apitest.CreateRandomCommit(t, dbc, 5, 700, nil)
	sum2, _ := apitest.CreateRandomCommit(t, dbc, 5, 700, [][]byte{sum1})
	sum3, c3 := apitest.CreateRandomCommit(t, dbc, 5, 700, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum3, c3))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {Sum: payload.BytesToHex(sum3)},
	}
	updates = apitest.PushObjects(t, dbc, rsc, updates, remoteRefs, 1024)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1, sum2, sum3})
	assertRefEqual(t, rs, "heads/alpha", sum3)
}
