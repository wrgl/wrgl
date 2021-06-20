// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

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
		Force  bool
		Negate bool
		IsGlob bool
	}{
		{"+refs/heads/*:refs/remotes/origin/*", "refs/heads/*", "refs/remotes/origin/*", true, false, true},
		{"refs/heads/main:refs/remotes/origin/main", "refs/heads/main", "refs/remotes/origin/main", false, false, false},
		{"refs/heads/main", "refs/heads/main", "", false, false, false},
		{"^refs/heads/qa*", "refs/heads/qa*", "", false, true, true},
		{"tag v1.0.*", "refs/tags/v1.0.*", "refs/tags/v1.0.*", false, false, true},
		{"main~4:refs/heads/main", "main~4", "refs/heads/main", false, false, false},
		{":refs/heads/main", "", "refs/heads/main", false, false, false},
	} {
		rs, err := ParseRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Src, rs.Src(), "case %d", i)
		assert.Equal(t, c.Dst, rs.Dst(), "case %d", i)
		assert.Equal(t, c.Force, rs.Force, "case %d", i)
		assert.Equal(t, c.Negate, rs.Negate, "case %d", i)
		b, err := rs.MarshalText()
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Text, string(b), "case %d", i)
		assert.Equal(t, c.IsGlob, rs.IsGlob())
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
		rs, err := ParseRefspec(c.Text)
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
		rs, err := ParseRefspec(c.Text)
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
		rs, err := ParseRefspec(c.Text)
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
		rs, err := ParseRefspec(c.Text)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Exclude, rs.Exclude(c.Ref), "case %d", i)
	}
}

func TestNewRefspec(t *testing.T) {
	for i, c := range []struct {
		Src    string
		Dst    string
		Negate bool
		Force  bool
		Result string
	}{
		{"main~4", "refs/heads/main", false, false, "main~4:refs/heads/main"},
		{"main^", "refs/tags/dec-2020", true, false, "^main^:refs/tags/dec-2020"},
		{"refs/heads/tickets", "refs/remotes/origin/tickets", false, true, "+refs/heads/tickets:refs/remotes/origin/tickets"},
		{"refs/heads/tickets", "", false, true, "+refs/heads/tickets"},
	} {
		rs, err := NewRefspec(c.Src, c.Dst, c.Negate, c.Force)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, c.Result, rs.String(), "case %d", i)
	}
}
