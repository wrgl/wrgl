// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestReflogWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	oid := testutils.SecureRandomBytes(16)
	records := []*Reflog{
		{
			NewOID:      oid,
			AuthorName:  "John Doe",
			AuthorEmail: "john@doe.com",
			Time:        time.Now().Round(time.Second),
			Action:      "commit",
			Message:     "initial commit",
		},
		{
			OldOID:     oid,
			NewOID:     testutils.SecureRandomBytes(16),
			AuthorName: "Jane Lane",
			Time:       time.Now().Add(24 * time.Hour).Round(time.Second),
			Action:     "commit",
			Message:    "add missing rows",
		},
	}
	for _, rec := range records {
		buf.Reset()
		n, err := rec.WriteTo(buf)
		require.NoError(t, err)
		assert.Len(t, buf.Bytes(), int(n))

		obj := &Reflog{}
		m, err := obj.Read(buf.Bytes())
		require.NoError(t, err)
		assert.Equal(t, int(n), m)
		assert.Equal(t, rec.OldOID, rec.OldOID)
		assert.Equal(t, rec.NewOID, rec.NewOID)
		assert.Equal(t, rec.AuthorName, rec.AuthorName)
		assert.Equal(t, rec.AuthorEmail, rec.AuthorEmail)
		assert.Equal(t, rec.Action, rec.Action)
		assert.Equal(t, rec.Message, rec.Message)
		assert.True(t, rec.Time.Equal(rec.Time))
	}
}
