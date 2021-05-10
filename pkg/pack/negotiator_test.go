package pack

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func TestNegotiatorHandleUploadPackRequest(t *testing.T) {
	db := kv.NewMockStore(false)
	sum1, _ := versioning.SaveTestCommit(t, db, nil)
	sum2, _ := versioning.SaveTestCommit(t, db, nil)
	sum3, c3 := versioning.SaveTestCommit(t, db, [][]byte{sum1})
	sum4, c4 := versioning.SaveTestCommit(t, db, [][]byte{sum2})
	sum5, _ := versioning.SaveTestCommit(t, db, [][]byte{sum3})
	sum6, _ := versioning.SaveTestCommit(t, db, [][]byte{sum4})
	require.NoError(t, versioning.SaveHead(db, "main", sum5))
	require.NoError(t, versioning.SaveTag(db, "v1", sum6))

	neg := NewNegotiator()
	acks, err := neg.HandleUploadPackRequest(db, [][]byte{sum3, sum4}, [][]byte{sum1, sum2}, false)
	require.NoError(t, err)
	// acks is nil mean no more negotiation needed
	assert.Empty(t, acks)
	commits := neg.CommitsToSend()
	assert.Len(t, commits, 2)
	objects.AssertCommitEqual(t, c3, commits[0])
	objects.AssertCommitEqual(t, c4, commits[1])
}

func TestNegotiatorSendACKs(t *testing.T) {
	db := kv.NewMockStore(false)
	sum1, _ := versioning.SaveTestCommit(t, db, nil)
	sum2, _ := versioning.SaveTestCommit(t, db, nil)
	sum3, c3 := versioning.SaveTestCommit(t, db, [][]byte{sum1})
	sum4, c4 := versioning.SaveTestCommit(t, db, [][]byte{sum2})
	sum5, c5 := versioning.SaveTestCommit(t, db, [][]byte{sum3})
	sum6, c6 := versioning.SaveTestCommit(t, db, [][]byte{sum4})
	require.NoError(t, versioning.SaveHead(db, "main", sum5))
	require.NoError(t, versioning.SaveTag(db, "v1", sum6))

	neg := NewNegotiator()
	acks, err := neg.HandleUploadPackRequest(db, [][]byte{sum5, sum6}, [][]byte{sum1}, false)
	require.NoError(t, err)
	// ACK sum1
	assert.Equal(t, [][]byte{sum1}, acks)
	acks, err = neg.HandleUploadPackRequest(db, nil, [][]byte{sum2}, false)
	require.NoError(t, err)
	// server has found closed set of objects, therefore acks is nil
	assert.Empty(t, acks)
	commits := neg.CommitsToSend()
	assert.Len(t, commits, 4)
	objects.AssertCommitEqual(t, c5, commits[0])
	objects.AssertCommitEqual(t, c3, commits[1])
	objects.AssertCommitEqual(t, c6, commits[2])
	objects.AssertCommitEqual(t, c4, commits[3])
}

func TestNegotiatorFoundUnrecognizedWants(t *testing.T) {
	db := kv.NewMockStore(false)
	sum1, _ := versioning.SaveTestCommit(t, db, nil)
	sum2, _ := versioning.SaveTestCommit(t, db, [][]byte{sum1})
	require.NoError(t, versioning.SaveHead(db, "main", sum2))
	sum3 := testutils.SecureRandomBytes(16)
	neg := NewNegotiator()
	_, err := neg.HandleUploadPackRequest(db, nil, [][]byte{sum1}, false)
	assert.Error(t, err, "empty wants list")
	_, err = neg.HandleUploadPackRequest(db, [][]byte{sum3}, [][]byte{sum1}, false)
	assert.Error(t, err, "unrecognized wants: "+hex.EncodeToString(sum3))
}
