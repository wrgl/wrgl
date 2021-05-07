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
		{"refs/heads/main:refs/remotes/origin/main", "refs/heads/main", "refs/remotes/origin/main", false, false},
		{"refs/heads/main", "refs/heads/main", "", false, false},
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

func TestSrcMatchRef(t *testing.T) {
	for i, c := range []struct {
		Text  string
		Ref   string
		Match bool
	}{
		{"+refs/heads/*:refs/remotes/origin/*", "refs/heads/abc", true},
		{"+refs/heads/*:refs/remotes/origin/*", "refs/tags/pdw", false},
		{"refs/heads/abc", "refs/heads/abc", true},
		{"refs/heads/main:refs/remotes/origin/mymain", "refs/remotes/origin/mymain", false},
	} {
		rs, err := NewRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Match, rs.SrcMatchRef(c.Ref), "case %d", i)
	}
}

func TestDstMatchRef(t *testing.T) {
	for i, c := range []struct {
		Text  string
		Ref   string
		Match bool
	}{
		{"+refs/heads/*:refs/remotes/origin/*", "refs/remotes/origin/abc", true},
		{"+refs/heads/*:refs/remotes/origin/*", "refs/tags/pdw", false},
		{"refs/heads/abc", "refs/heads/abc", false},
		{"refs/heads/main:refs/remotes/origin/mymain", "refs/remotes/origin/mymain", true},
		{"refs/heads/main:refs/remotes/origin/mymain", "refs/heads/qwer", false},
	} {
		rs, err := NewRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Match, rs.DstMatchRef(c.Ref), "case %d", i)
	}
}

func TestDstForRef(t *testing.T) {
	for i, c := range []struct {
		Text string
		Ref  string
		Dst  string
	}{
		{"+refs/heads/*:refs/remotes/origin/*", "refs/heads/abc", "refs/remotes/origin/abc"},
		{"+refs/heads/*:refs/remotes/origin/*", "refs/tags/pdw", ""},
		{"refs/heads/abc", "refs/heads/abc", ""},
		{"refs/heads/main:refs/remotes/origin/mymain", "refs/heads/main", "refs/remotes/origin/mymain"},
		{"refs/heads/main:refs/remotes/origin/mymain", "refs/heads/qwer", ""},
	} {
		rs, err := NewRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Dst, rs.DstForRef(c.Ref), "case %d", i)
	}
}

func TestRefspecExclude(t *testing.T) {
	for i, c := range []struct {
		Text    string
		Ref     string
		Exclude bool
	}{
		{"^refs/heads/m*", "refs/heads/main", true},
		{"^refs/heads/m*", "refs/heads/tickets", false},
		{"^refs/heads/main", "refs/heads/main", true},
		{"^refs/heads/main", "refs/heads/abc", false},
		{"+refs/tags/*:refs/remotes/origin/tags/*", "refs/heads/main", false},
	} {
		rs, err := NewRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Exclude, rs.Exclude(c.Ref), "case %d", i)
	}
}
