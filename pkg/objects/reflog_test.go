// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestReflogWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := NewReflogWriter(buf)
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
		err := w.Write(rec)
		require.NoError(t, err)
	}

	r, err := NewReflogReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	for i := 1; i >= 0; i-- {
		rec, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, records[i].OldOID, rec.OldOID)
		assert.Equal(t, records[i].NewOID, rec.NewOID)
		assert.Equal(t, records[i].AuthorName, rec.AuthorName)
		assert.Equal(t, records[i].AuthorEmail, rec.AuthorEmail)
		assert.Equal(t, records[i].Action, rec.Action)
		assert.Equal(t, records[i].Message, rec.Message)
		assert.True(t, records[i].Time.Equal(rec.Time))
	}
	_, err = r.Read()
	assert.Equal(t, io.EOF, err)
}
