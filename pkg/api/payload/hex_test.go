// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
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
