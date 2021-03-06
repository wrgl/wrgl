// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package authfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/auth/random"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestIDToken(t *testing.T) {
	sec := []byte(random.RandomAlphaNumericString(10))
	email := "test@domain.com"
	name := "John Doe"
	ts, err := createIDToken(email, name, sec, 100*time.Millisecond)
	require.NoError(t, err)
	c, err := validateIDToken(ts, sec)
	require.NoError(t, err)
	assert.Equal(t, email, c.Email)
	assert.Equal(t, name, c.Name)
	_, err = validateIDToken(ts, testutils.SecureRandomBytes(20))
	assert.Error(t, err)

	time.Sleep(2 * time.Second)

	_, err = validateIDToken(ts, sec)
	assert.Error(t, err)
}
