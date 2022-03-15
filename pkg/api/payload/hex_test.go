// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package payload

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestMarshalHex(t *testing.T) {
	b := Hex(testutils.RandomSum())
	s, err := json.Marshal(&b)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(`"%x"`, b), string(s))
	h := &Hex{}
	require.NoError(t, json.Unmarshal(s, &h))
	assert.Equal(t, b, *h)
}

func TestAppendHex(t *testing.T) {
	b1 := testutils.SecureRandomBytes(16)
	b2 := testutils.SecureRandomBytes(16)
	h := BytesToHex(b1)
	assert.Equal(t, b1, (*h)[:])
	assert.Nil(t, BytesToHex(nil))
	sl := AppendHex(nil, b1)
	assert.Len(t, sl, 1)
	sl = AppendHex(sl, b2)
	assert.Len(t, sl, 2)
	sl = AppendHex(sl, nil)
	assert.Len(t, sl, 3)
	assert.Equal(t, b1, (*sl[0])[:])
	assert.Equal(t, b2, (*sl[1])[:])
	assert.Equal(t, [][]byte{b1, b2, nil}, HexSliceToBytesSlice(sl))
	sl[0] = nil
	assert.Equal(t, [][]byte{nil, b2, nil}, HexSliceToBytesSlice(sl))
}
