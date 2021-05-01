package objects

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestWriteCommit(t *testing.T) {
	buf := bytes.NewBufferString("")
	w := NewCommitWriter(buf)
	r := NewCommitReader(buf)
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
	err := w.Write(c)
	require.NoError(t, err)
	c2, err := r.Read()
	require.NoError(t, err)
	AssertCommitEqual(t, c, c2)
}
