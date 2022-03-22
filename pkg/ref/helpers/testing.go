// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package refhelpers

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

var tg = testutils.CreateTimeGen()

func SaveTestCommit(t *testing.T, db objects.Store, parents [][]byte) (sum []byte, commit *objects.Commit) {
	t.Helper()
	commit = &objects.Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        tg(),
		Message:     testutils.BrokenRandomAlphaNumericString(40),
		Parents:     parents,
	}
	buf := bytes.NewBuffer(nil)
	_, err := commit.WriteTo(buf)
	require.NoError(t, err)
	sum, err = objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	commit.Sum = sum
	return sum, commit
}

func AssertLatestReflogEqual(t *testing.T, s ref.Store, name string, rl *ref.Reflog) {
	t.Helper()
	r, err := s.LogReader(name)
	require.NoError(t, err)
	defer r.Close()
	obj, err := r.Read()
	require.NoError(t, err)
	AssertReflogEqual(t, rl, obj)
}

func RandomReflog() *ref.Reflog {
	return &ref.Reflog{
		OldOID:      testutils.SecureRandomBytes(16),
		NewOID:      testutils.SecureRandomBytes(16),
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(10),
		Time:        time.Now(),
		Action:      testutils.BrokenRandomLowerAlphaString(5),
		Message:     testutils.BrokenRandomLowerAlphaString(10),
	}
}

func AssertReflogEqual(t *testing.T, a, b *ref.Reflog) {
	t.Helper()
	assert.Equal(t, a.OldOID, b.OldOID, "OldOID not equal")
	assert.Equal(t, a.NewOID, b.NewOID, "NewOID not equal")
	assert.Equal(t, a.AuthorName, b.AuthorName, "AuthorName not equal")
	assert.Equal(t, a.AuthorEmail, b.AuthorEmail, "AuthorEmail not equal")
	assert.Equal(t, a.Action, b.Action, "Action not equal")
	assert.Equal(t, a.Message, b.Message, "Message not equal")
	assert.Equal(t, a.Txid, b.Txid, "Txid not equal")
	if !a.Time.IsZero() {
		assert.Equal(t, objline.EncodeTime(a.Time), objline.EncodeTime(b.Time), "Time not equal")
	}
}

func AssertReflogReaderContains(t *testing.T, rs ref.Store, name string, logs ...*ref.Reflog) {
	t.Helper()
	reader, err := rs.LogReader(name)
	require.NoError(t, err)
	defer reader.Close()
	for _, l := range logs {
		v, err := reader.Read()
		require.NoError(t, err)
		AssertReflogEqual(t, l, v)
	}
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func AssertTransactionEqual(t *testing.T, a, b *ref.Transaction) {
	t.Helper()
	require.Equal(t, a.ID, b.ID, "id not equal")
	require.Equal(t, a.Status, b.Status, "status not equal")
	testutils.AssertTimeEqual(t, a.Begin, b.Begin, "begin not equal")
	testutils.AssertTimeEqual(t, a.End, b.End, "end not equal")
}

func AssertTransactionSliceEqual(t *testing.T, a, b []*ref.Transaction) {
	t.Helper()
	require.Len(t, a, len(b))
	for i, v := range a {
		AssertTransactionEqual(t, v, b[i])
	}
}
