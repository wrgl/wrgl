package objects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func AssertCommitEqual(t *testing.T, a, b *Commit) {
	assert.Equal(t, a.Table, b.Table)
	assert.Equal(t, a.AuthorName, b.AuthorName)
	assert.Equal(t, a.AuthorEmail, b.AuthorEmail)
	assert.Equal(t, a.Message, b.Message)
	assert.Equal(t, a.Parents, b.Parents)
	assert.Equal(t, a.Time.Unix(), b.Time.Unix())
	assert.Equal(t, a.Time.Format("-0700"), b.Time.Format("-0700"))
}
