// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package refhelpers

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
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

func AssertLatestReflogEqual(t *testing.T, s ref.Store, name string, rl *objects.Reflog) {
	t.Helper()
	r, err := s.LogReader(name)
	require.NoError(t, err)
	defer r.Close()
	obj, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, rl.OldOID, obj.OldOID)
	assert.Equal(t, rl.NewOID, obj.NewOID)
	assert.Equal(t, rl.AuthorName, obj.AuthorName)
	assert.Equal(t, rl.AuthorEmail, obj.AuthorEmail)
	assert.Equal(t, rl.Action, obj.Action)
	assert.Equal(t, rl.Message, obj.Message)
}
