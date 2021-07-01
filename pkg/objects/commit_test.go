// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestWriteCommit(t *testing.T) {
	c := &Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        time.Now(),
		Message:     "new commit",
		Parents: [][]byte{
			testutils.SecureRandomBytes(16),
		},
	}
	buf := bytes.NewBufferString("")
	n, err := c.WriteTo(buf)
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	n, c2, err := ReadCommitFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	AssertCommitEqual(t, c, c2)
}
