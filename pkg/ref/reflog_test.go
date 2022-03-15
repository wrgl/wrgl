// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestReflogWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	oid := testutils.SecureRandomBytes(16)
	newOID := testutils.SecureRandomBytes(16)
	for _, tc := range []struct {
		Reflog *Reflog
		S      string
	}{
		{
			Reflog: &Reflog{
				NewOID:      oid,
				AuthorName:  "John Doe",
				AuthorEmail: "john@doe.com",
				Time:        time.Date(2022, 1, 18, 16, 0, 0, 0, time.FixedZone("ICT", 7*3600)),
				Action:      "commit",
				Message:     "initial commit",
			},
			S: fmt.Sprintf("00000000000000000000000000000000 %x John Doe <john@doe.com> 1642496400 +0700 commit: initial commit", oid),
		},
		{
			Reflog: &Reflog{
				OldOID:     oid,
				NewOID:     newOID,
				AuthorName: "Jane Lane",
				Time:       time.Date(2022, 1, 19, 16, 0, 0, 0, time.FixedZone("ICT", 7*3600)),
				Action:     "commit",
				Message:    "add missing rows",
			},
			S: fmt.Sprintf("%x %x Jane Lane 1642582800 +0700 commit: add missing rows", oid, newOID),
		},
	} {
		buf.Reset()
		n, err := tc.Reflog.WriteTo(buf)
		require.NoError(t, err)
		assert.Len(t, buf.Bytes(), int(n))
		assert.Equal(t, tc.S, buf.String())

		obj := &Reflog{}
		m, err := obj.Read(buf.Bytes())
		require.NoError(t, err)
		assert.Equal(t, int(n), m)
		assert.Equal(t, tc.Reflog.OldOID, obj.OldOID)
		assert.Equal(t, tc.Reflog.NewOID, obj.NewOID)
		assert.Equal(t, tc.Reflog.AuthorName, obj.AuthorName)
		assert.Equal(t, tc.Reflog.AuthorEmail, obj.AuthorEmail)
		assert.Equal(t, tc.Reflog.Action, obj.Action)
		assert.Equal(t, tc.Reflog.Message, obj.Message)
		assert.True(t, tc.Reflog.Time.Equal(obj.Time))
	}
}

func TestReflogFetchRemote(t *testing.T) {
	assert.Equal(t, "origin", (&Reflog{
		NewOID:      testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "[from origin] storing head",
	}).FetchRemote())
	assert.Equal(t, "", (&Reflog{
		NewOID:      testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "commit",
		Message:     "new data",
	}).FetchRemote())
}
