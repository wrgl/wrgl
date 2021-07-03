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
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	refmock "github.com/wrgl/core/pkg/ref/mock"
	"github.com/wrgl/core/pkg/testutils"
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
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", NewReceivePackHandler(db, rs, packtest.ReceivePackConfig(false, false)),
	)

	dbc := objmock.NewStore()
	packtest.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1})
	sum4, c4 := factory.CommitRandom(t, dbc, [][]byte{sum1})
	sum5, c5 := factory.CommitRandom(t, dbc, nil)
	sum6, c6 := factory.CommitRandom(t, dbc, nil)
	sum8, c8 := factory.CommitRandom(t, dbc, nil)
	client, err := packclient.NewClient(dbc, packtest.TestOrigin)
	require.NoError(t, err)

	updates := []*packutils.Update{
		{Dst: "refs/heads/alpha", OldSum: sum1, Sum: sum4},                            // fast-forward
		{Dst: "refs/heads/beta", OldSum: sum2},                                        // delete
		{Dst: "refs/heads/gamma", Sum: sum5},                                          // create
		{Dst: "refs/heads/delta", OldSum: testutils.SecureRandomBytes(16), Sum: sum6}, // outdated ref
		{Dst: "refs/heads/theta", OldSum: sum7, Sum: sum8},                            // non-fast-forward
	}
	require.NoError(t, client.PostReceivePack(updates, []*objects.Commit{c4, c5, c6, c8}, nil))
	assert.Empty(t, updates[0].ErrMsg)
	assert.Empty(t, updates[1].ErrMsg)
	assert.Empty(t, updates[2].ErrMsg)
	assert.Equal(t, "remote ref updated since checkout", updates[3].ErrMsg)
	assert.Empty(t, updates[4].ErrMsg)
	packtest.AssertCommitsPersisted(t, db, rs, [][]byte{sum1, sum4, sum5})
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
}

func TestReceivePackHandlerNoDeletesNoFastForwards(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", NewReceivePackHandler(db, rs, packtest.ReceivePackConfig(true, true)),
	)

	dbc := objmock.NewStore()
	sum3, c3 := factory.CommitRandom(t, dbc, nil)
	client, err := packclient.NewClient(dbc, packtest.TestOrigin)
	require.NoError(t, err)

	updates := []*packutils.Update{
		{Dst: "refs/heads/alpha", OldSum: sum1, Sum: sum3},
		{Dst: "refs/heads/beta", OldSum: sum2},
	}
	require.NoError(t, client.PostReceivePack(updates, []*objects.Commit{c3}, nil))
	assert.Equal(t, "remote does not support non-fast-fowards", updates[0].ErrMsg)
	assert.Equal(t, "remote does not support deleting refs", updates[1].ErrMsg)
	assertRefEqual(t, rs, "heads/alpha", sum1)
	assertRefEqual(t, rs, "heads/beta", sum2)
}
