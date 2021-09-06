// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/auth"
)

func TestAuthzStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewAuthzStore(dir)
	require.NoError(t, err)

	email1 := "alice@domain.com"
	email2 := "bob@domain.com"
	require.NoError(t, s.AddPolicy(email1, auth.ScopeRead))
	require.NoError(t, s.AddPolicy(email2, auth.ScopeRead))
	require.NoError(t, s.AddPolicy(email2, auth.ScopeWrite))

	ok, err := s.Authorized(email1, auth.ScopeRead)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = s.Authorized(email1, auth.ScopeWrite)
	require.NoError(t, err)
	assert.False(t, ok)
	ok, err = s.Authorized(email2, auth.ScopeWrite)
	require.NoError(t, err)
	assert.True(t, ok)

	scopes, err := s.ListPolicies(email1)
	require.NoError(t, err)
	assert.Equal(t, []string{auth.ScopeRead}, scopes)
	scopes, err = s.ListPolicies(email2)
	require.NoError(t, err)
	assert.Equal(t, []string{auth.ScopeRead, auth.ScopeWrite}, scopes)

	require.NoError(t, s.Flush())

	s, err = NewAuthzStore(dir)
	require.NoError(t, err)
	ok, err = s.Authorized(email1, auth.ScopeRead)
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, s.RemovePolicy(email1, auth.ScopeRead))
	ok, err = s.Authorized(email1, auth.ScopeRead)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, s.Flush())

	s, err = NewAuthzStore(dir)
	require.NoError(t, err)
	ok, err = s.Authorized(email1, auth.ScopeRead)
	require.NoError(t, err)
	assert.False(t, ok)
}
