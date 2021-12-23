// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestAuthzStore(t *testing.T) {
	dir, err := testutils.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	rd := local.NewRepoDir(dir, "")
	defer rd.Close()

	s, err := NewAuthzStore(rd)
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

	s, err = NewAuthzStore(rd)
	require.NoError(t, err)
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, s.RemovePolicy(email1, auth.ScopeRepoRead))
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, s.Flush())

	s, err = NewAuthzStore(rd)
	require.NoError(t, err)
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, s.AddPolicy(auth.Anyone, auth.ScopeRepoRead))
	ok, err = s.Authorized(r, email1, auth.ScopeRepoRead)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAuthzStoreWatchFile(t *testing.T) {
	dir, err := testutils.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	rd := local.NewRepoDir(dir, "")
	defer rd.Close()

	s, err := NewAuthzStore(rd)
	require.NoError(t, err)
	defer s.Close()

	f, err := os.Create(filepath.Join(dir, "authz.csv"))
	require.NoError(t, err)
	_, err = f.Write([]byte(fmt.Sprintf("p, john.doe@domain.com, -, %s\n", auth.ScopeRepoRead)))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	time.Sleep(time.Millisecond * 100)
	scopes, err := s.ListPolicies("john.doe@domain.com")
	require.NoError(t, err)
	assert.Equal(t, []string{auth.ScopeRepoRead}, scopes)
}
