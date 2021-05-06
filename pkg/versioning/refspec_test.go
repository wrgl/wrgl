package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefSpec(t *testing.T) {
	for i, c := range []struct {
		Text   string
		Src    string
		Dst    string
		Plus   bool
		Negate bool
	}{
		{"+refs/heads/*:refs/remotes/origin/*", "refs/heads/*", "refs/remotes/origin/*", true, false},
		{"refs/heads/master:refs/remotes/origin/master", "refs/heads/master", "refs/remotes/origin/master", false, false},
		{"refs/heads/master", "refs/heads/master", "", false, false},
		{"^refs/heads/qa*", "refs/heads/qa*", "", false, true},
		{"tag v1.0.*", "refs/tags/v1.0.*", "refs/tags/v1.0.*", false, false},
	} {
		rs, err := NewRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Src, rs.Src(), "case %d", i)
		assert.Equal(t, c.Dst, rs.Dst(), "case %d", i)
		assert.Equal(t, c.Plus, rs.Plus, "case %d", i)
		assert.Equal(t, c.Negate, rs.Negate, "case %d", i)
		b, err := rs.MarshalText()
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Text, string(b), "case %d", i)
	}
}
