// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package flatdb

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

	peoples := make([][]string, 10)
	for i := range peoples {
		peoples[i] = []string{
			fmt.Sprintf("%s@%s.com", testutils.BrokenRandomLowerAlphaString(8), testutils.BrokenRandomLowerAlphaString(8)),
			testutils.BrokenRandomAlphaNumericString(10),
		}
		require.NoError(t, s.SetPassword(peoples[i][0], peoples[i][1]))
	}

	tokens := make([]string, len(peoples))
	for i, sl := range peoples {
		ts, err := s.Authenticate(sl[0], sl[1])
		require.NoError(t, err)
		tokens[i] = ts
		_, err = s.Authenticate(sl[0], testutils.BrokenRandomAlphaNumericString(10))
		assert.Error(t, err)
	}
	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(dir, 1*time.Second)
	require.NoError(t, err)
	for i, sl := range peoples {
		c, err := s.CheckToken(tokens[i])
		require.NoError(t, err)
		assert.Equal(t, sl[0], c.Email)
	}

	time.Sleep(2 * time.Second)

	for i := range peoples {
		_, err := s.CheckToken(tokens[i])
		assert.Error(t, err)
	}

	for _, sl := range peoples {
		require.NoError(t, s.RemoveUser(sl[0]))
		_, err = s.Authenticate(sl[0], sl[1])
		assert.Error(t, err)
	}
	require.NoError(t, s.Flush())

	s, err = NewAuthnStore(dir, 0)
	require.NoError(t, err)
	for _, sl := range peoples {
		_, err = s.Authenticate(sl[0], sl[1])
		assert.Error(t, err)
	}
}
