// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestAuthnStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewAuthnStore(dir, 1*time.Second)
	require.NoError(t, err)

	peoples := map[string]string{}
	for i := 0; i < 10; i++ {
		email := fmt.Sprintf("%s@%s.com", testutils.BrokenRandomLowerAlphaString(8), testutils.BrokenRandomLowerAlphaString(8))
		pass := testutils.BrokenRandomAlphaNumericString(10)
		peoples[email] = pass
		require.NoError(t, s.SetPassword(email, pass))
		assert.True(t, s.Exist(email))
	}

	tokens := map[string]string{}
	for email, pass := range peoples {
		ts, err := s.Authenticate(email, pass)
		require.NoError(t, err)
		tokens[email] = ts
		_, err = s.Authenticate(email, testutils.BrokenRandomAlphaNumericString(10))
		assert.Error(t, err)
	}
	emails, err := s.ListUsers()
	require.NoError(t, err)
	assert.Len(t, emails, 10)
	for _, email := range emails {
		_, ok := peoples[email]
		assert.True(t, ok)
	}

	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(dir, 3*time.Second)
	require.NoError(t, err)
	start := time.Now()
	for email := range peoples {
		assert.True(t, s.Exist(email))
		c, err := s.CheckToken(tokens[email])
		require.NoError(t, err)
		assert.Equal(t, email, c.Email)
	}

	time.Sleep(5*time.Second - time.Since(start))

	for email := range peoples {
		_, err := s.CheckToken(tokens[email])
		assert.Error(t, err)
	}

	for email, pass := range peoples {
		require.NoError(t, s.RemoveUser(email))
		_, err = s.Authenticate(email, pass)
		assert.Error(t, err)
		assert.False(t, s.Exist(email))
	}
	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(dir, 0)
	require.NoError(t, err)
	for email, pass := range peoples {
		_, err = s.Authenticate(email, pass)
		assert.Error(t, err)
		assert.False(t, s.Exist(email))
	}
}
