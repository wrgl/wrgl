// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestSaveRows(t *testing.T) {
	s := kv.NewMockStore(false)
	err := SaveRow(s, []byte("abcasdf"), []byte("1231234"))
	require.NoError(t, err)
	err = SaveRow(s, []byte("defqwer"), []byte("4564567"))
	require.NoError(t, err)

	m, err := GetRows(s, [][]byte{[]byte("abcasdf"), []byte("defqwer")})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("1231234"),
		[]byte("4564567"),
	}, m)

	sl, err := GetAllRowKeys(s)
	require.NoError(t, err)
	assert.Equal(t, []string{"abcasdf", "defqwer"}, sl)

	err = DeleteRow(s, []byte("abcasdf"))
	require.NoError(t, err)
	_, err = GetRows(s, [][]byte{[]byte("abcasdf")})
	assert.Equal(t, kv.KeyNotFoundError, err)
	m, err = GetRows(s, [][]byte{[]byte("defqwer")})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("4564567"),
	}, m)
}
