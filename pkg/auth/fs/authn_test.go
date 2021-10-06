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
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestAuthnStore(t *testing.T) {
	dir, err := testutils.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	rd := local.NewRepoDir(dir, "")
	defer rd.Close()

	s, err := NewAuthnStore(rd, 0)
	require.NoError(t, err)

	peoples := map[string][]string{}
	for i := 0; i < 10; i++ {
		email := fmt.Sprintf("%s@%s.com", testutils.BrokenRandomLowerAlphaString(8), testutils.BrokenRandomLowerAlphaString(8))
		pass := testutils.BrokenRandomAlphaNumericString(10)
		name := testutils.BrokenRandomLowerAlphaString(5)
		peoples[email] = []string{name, pass}
		require.NoError(t, s.SetPassword(email, pass))
		require.NoError(t, s.SetName(email, name))
		assert.True(t, s.Exist(email))
	}
	t.Logf("peoples: %v", peoples)

	tokens := map[string]string{}
	for email, sl := range peoples {
		pass := sl[1]
		ts, err := s.Authenticate(email, pass)
		require.NoError(t, err)
		tokens[email] = ts
		_, err = s.Authenticate(email, testutils.BrokenRandomAlphaNumericString(10))
		assert.Error(t, err)
	}
	users, err := s.ListUsers()
	require.NoError(t, err)
	assert.Len(t, users, 10)
	for _, sl := range users {
		email, name := sl[0], sl[1]
		sl, ok := peoples[email]
		assert.True(t, ok)
		assert.Equal(t, name, sl[0])
	}

	t.Logf("internal slice: %v", s.sl)
	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(rd, 0)
	require.NoError(t, err)
	for email, sl := range peoples {
		name := sl[0]
		assert.True(t, s.Exist(email))
		r, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)
		req, c, err := s.CheckToken(r, tokens[email])
		assert.Equal(t, r, req)
		require.NoError(t, err)
		assert.Equal(t, email, c.Email)
		assert.Equal(t, name, c.Name)
	}

	for email, sl := range peoples {
		pass := sl[1]
		require.NoError(t, s.RemoveUser(email))
		_, err = s.Authenticate(email, pass)
		assert.Error(t, err)
		assert.False(t, s.Exist(email))
	}
	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(rd, 0)
	require.NoError(t, err)
	for email, sl := range peoples {
		pass := sl[1]
		_, err = s.Authenticate(email, pass)
		assert.Error(t, err)
		assert.False(t, s.Exist(email))
	}
}

func TestAuthnStoreWatchFile(t *testing.T) {
	dir, err := testutils.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	rd := local.NewRepoDir(dir, "")
	defer rd.Close()

	s, err := NewAuthnStore(rd, 0)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(dir, "authn.csv"))
	require.NoError(t, err)
	_, err = f.Write([]byte("john.doe@domain.com,John Doe,password"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	time.Sleep(time.Millisecond * 100)
	sl, err := s.ListUsers()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"john.doe@domain.com", "John Doe"},
	}, sl)
}
