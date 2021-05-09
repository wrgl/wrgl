package objects

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func AssertCommitEqual(t *testing.T, a, b *Commit) {
	t.Helper()
	require.Equal(t, a.Table, b.Table)
	require.Equal(t, a.AuthorName, b.AuthorName)
	require.Equal(t, a.AuthorEmail, b.AuthorEmail)
	require.Equal(t, a.Message, b.Message)
	require.Equal(t, a.Parents, b.Parents)
	require.Equal(t, a.Time.Unix(), b.Time.Unix())
	require.Equal(t, a.Time.Format("-0700"), b.Time.Format("-0700"))
}
