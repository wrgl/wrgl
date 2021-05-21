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
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func receivePackConfig(denyNonFastForwards, denyDeletes bool) *versioning.Config {
	return &versioning.Config{
		User: &versioning.ConfigUser{
			Name:  "test",
			Email: "test@domain.com",
		},
		Receive: &versioning.ConfigReceive{
			DenyNonFastForwards: denyNonFastForwards,
			DenyDeletes:         denyDeletes,
		},
	}
}

func assertRefEqual(t *testing.T, db kv.DB, ref string, sum []byte) {
	t.Helper()
	b, err := versioning.GetRef(db, ref)
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
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, _ := factory.CommitHead(t, db, fs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, fs, "beta", nil, nil)
	sum3, _ := factory.CommitHead(t, db, fs, "delta", nil, nil)
	sum7, _ := factory.CommitHead(t, db, fs, "theta", nil, nil)
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", NewReceivePackHandler(db, fs, receivePackConfig(false, false)),
	)

	dbc := kv.NewMockStore(false)
	fsc := kv.NewMockStore(false)
	packtest.CopyCommitsToNewStore(t, db, dbc, fs, fsc, [][]byte{sum1})
	sum4, c4 := factory.CommitRandom(t, dbc, fsc, [][]byte{sum1})
	sum5, c5 := factory.CommitRandom(t, dbc, fsc, nil)
	sum6, c6 := factory.CommitRandom(t, dbc, fsc, nil)
	sum8, c8 := factory.CommitRandom(t, dbc, fsc, nil)
	client, err := packclient.NewClient(dbc, fsc, packtest.TestOrigin)
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
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum1, sum4, sum5})
	assertRefEqual(t, db, "heads/alpha", sum4)
	versioning.AssertLatestReflogEqual(t, fs, "heads/alpha", &objects.Reflog{
		OldOID:      sum1,
		NewOID:      sum4,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "update ref",
	})
	assertRefEqual(t, db, "heads/beta", nil)
	assertRefEqual(t, db, "heads/gamma", sum5)
	versioning.AssertLatestReflogEqual(t, fs, "heads/gamma", &objects.Reflog{
		NewOID:      sum5,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "create ref",
	})
	assertRefEqual(t, db, "heads/delta", sum3)
	assertRefEqual(t, db, "heads/theta", sum8)
	versioning.AssertLatestReflogEqual(t, fs, "heads/theta", &objects.Reflog{
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
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, _ := factory.CommitHead(t, db, fs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, fs, "beta", nil, nil)
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", NewReceivePackHandler(db, fs, receivePackConfig(true, true)),
	)

	dbc := kv.NewMockStore(false)
	fsc := kv.NewMockStore(false)
	sum3, c3 := factory.CommitRandom(t, dbc, fsc, nil)
	client, err := packclient.NewClient(dbc, fsc, packtest.TestOrigin)
	require.NoError(t, err)

	updates := []*packutils.Update{
		{Dst: "refs/heads/alpha", OldSum: sum1, Sum: sum3},
		{Dst: "refs/heads/beta", OldSum: sum2},
	}
	require.NoError(t, client.PostReceivePack(updates, []*objects.Commit{c3}, nil))
	assert.Equal(t, "remote does not support non-fast-fowards", updates[0].ErrMsg)
	assert.Equal(t, "remote does not support deleting refs", updates[1].ErrMsg)
	assertRefEqual(t, db, "heads/alpha", sum1)
	assertRefEqual(t, db, "heads/beta", sum2)
}
