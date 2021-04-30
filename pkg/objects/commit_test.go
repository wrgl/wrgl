package objects

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func randomHash() [16]byte {
	b := testutils.SecureRandomBytes(16)
	arr := [16]byte{}
	copy(arr[:], b)
	return arr
}

func TestWriteCommit(t *testing.T) {
	buf := bytes.NewBufferString("")
	w := NewCommitWriter(buf)
	r := NewCommitReader(buf)
	c := &Commit{
		Table:       randomHash(),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        time.Now(),
		Message:     "new commit",
		Parents: [][16]byte{
			randomHash(),
		},
	}
	err := w.Write(c)
	require.NoError(t, err)
	c2, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, c.Table, c2.Table)
	assert.Equal(t, c.AuthorName, c2.AuthorName)
	assert.Equal(t, c.AuthorEmail, c2.AuthorEmail)
	assert.Equal(t, c.Message, c2.Message)
	assert.Equal(t, c.Parents, c2.Parents)
	assert.Equal(t, c.Time.Unix(), c2.Time.Unix())
	assert.Equal(t, c.Time.Format("-0700"), c2.Time.Format("-0700"))
}
