// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"io/ioutil"
	"net/http"
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
	require.NoError(t, s.AddPolicy(email1, auth.ScopeRepoRead))
	require.NoError(t, s.AddPolicy(email2, auth.ScopeRepoRead))
	require.NoError(t, s.AddPolicy(email2, auth.ScopeRepoWrite))

	r, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	ok, err := s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = s.Authorized(r, email1, auth.ScopeRepoWrite)
	require.NoError(t, err)
	assert.False(t, ok)
	ok, err = s.Authorized(r, email2, auth.ScopeRepoWrite)
	require.NoError(t, err)
	assert.True(t, ok)

	scopes, err := s.ListPolicies(email1)
	require.NoError(t, err)
	assert.Equal(t, []string{auth.ScopeRepoRead}, scopes)
	scopes, err = s.ListPolicies(email2)
	require.NoError(t, err)
	assert.Equal(t, []string{auth.ScopeRepoRead, auth.ScopeRepoWrite}, scopes)

	require.NoError(t, s.Flush())

	s, err = NewAuthzStore(dir)
	require.NoError(t, err)
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, s.RemovePolicy(email1, auth.ScopeRepoRead))
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, s.Flush())

	s, err = NewAuthzStore(dir)
	require.NoError(t, err)
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.False(t, ok)
}
