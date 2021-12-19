// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package refhelpers

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

var tg func() time.Time

func init() {
	tg = testutils.CreateTimeGen()
}

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
	if !a.Time.IsZero() {
		assert.Equal(t, objline.EncodeTime(a.Time), objline.EncodeTime(b.Time), "Time not equal")
	}
}
