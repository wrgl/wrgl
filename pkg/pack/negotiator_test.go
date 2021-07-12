// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	objhelpers "github.com/wrgl/core/pkg/objects/helpers"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	refmock "github.com/wrgl/core/pkg/ref/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestNegotiatorHandleUploadPackRequest(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, c1 := refhelpers.SaveTestCommit(t, db, nil)
	sum2, c2 := refhelpers.SaveTestCommit(t, db, nil)
	sum3, c3 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	sum4, c4 := refhelpers.SaveTestCommit(t, db, [][]byte{sum2})
	sum5, c5 := refhelpers.SaveTestCommit(t, db, [][]byte{sum3})
	sum6, c6 := refhelpers.SaveTestCommit(t, db, [][]byte{sum4})
	require.NoError(t, ref.CommitHead(rs, "main", sum5, c5))
	require.NoError(t, ref.SaveTag(rs, "v1", sum6))

	// send everything if haves are empty
	neg := NewNegotiator()
	acks, err := neg.HandleUploadPackRequest(db, rs, [][]byte{sum5, sum6}, nil, false)
	require.NoError(t, err)
	assert.Empty(t, acks)
	commits := neg.CommitsToSend()
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c1, c2, c3, c4, c5, c6}, commits, true)

	neg = NewNegotiator()
	acks, err = neg.HandleUploadPackRequest(db, rs, [][]byte{sum3, sum4}, [][]byte{sum1, sum2}, false)
	require.NoError(t, err)
	// acks is nil mean no more negotiation needed
	assert.Empty(t, acks)
	commits = neg.CommitsToSend()
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c3, c4}, commits, true)
}

func TestNegotiatorSendACKs(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := refhelpers.SaveTestCommit(t, db, nil)
	sum2, _ := refhelpers.SaveTestCommit(t, db, nil)
	sum3, _ := refhelpers.SaveTestCommit(t, db, nil)
	sum4, c4 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	sum5, c5 := refhelpers.SaveTestCommit(t, db, [][]byte{sum2, sum3})
	sum6, c6 := refhelpers.SaveTestCommit(t, db, [][]byte{sum4})
	sum7, c7 := refhelpers.SaveTestCommit(t, db, [][]byte{sum5})
	sum8, c8 := refhelpers.SaveTestCommit(t, db, nil)
	sum9, c9 := refhelpers.SaveTestCommit(t, db, [][]byte{sum8})
	require.NoError(t, ref.CommitHead(rs, "alpha", sum6, c6))
	require.NoError(t, ref.SaveTag(rs, "v1", sum7))
	require.NoError(t, ref.CommitHead(rs, "beta", sum9, c9))

	neg := NewNegotiator()
	acks, err := neg.HandleUploadPackRequest(db, rs, [][]byte{sum6, sum7, sum9}, [][]byte{sum1}, false)
	require.NoError(t, err)
	// ACK sum1
	assert.Equal(t, [][]byte{sum1}, acks)

	acks, err = neg.HandleUploadPackRequest(db, rs, nil, [][]byte{sum2}, false)
	require.NoError(t, err)
	// ACK sum2
	assert.Equal(t, [][]byte{sum2}, acks)

	acks, err = neg.HandleUploadPackRequest(db, rs, nil, [][]byte{sum3}, true)
	require.NoError(t, err)
	// done negotiating therefore no more ACKs
	assert.Empty(t, acks)

	commits := neg.CommitsToSend()
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c4, c5, c6, c7, c8, c9}, commits, true)
}

func TestNegotiatorFoundUnrecognizedWants(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, _ := refhelpers.SaveTestCommit(t, db, nil)
	sum2, c2 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))
	sum3 := testutils.SecureRandomBytes(16)
	neg := NewNegotiator()
	_, err := neg.HandleUploadPackRequest(db, rs, nil, [][]byte{sum1}, false)
	assert.Error(t, err, "empty wants list")
	_, err = neg.HandleUploadPackRequest(db, rs, [][]byte{sum3}, [][]byte{sum1}, false)
	assert.Error(t, err, "unrecognized wants: "+hex.EncodeToString(sum3))
}
