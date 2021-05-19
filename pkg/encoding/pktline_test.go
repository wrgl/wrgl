// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package encoding

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/testutils"
)

func TestPktLine(t *testing.T) {
	w := bytes.NewBuffer(nil)
	buf := misc.NewBuffer(nil)
	sl := []string{
		"",
		"abcd 1234",
		hex.EncodeToString(testutils.SecureRandomBytes(16)) + " refs/heads/tickets",
		"",
	}
	for _, s := range sl {
		err := WritePktLine(w, buf, s)
		require.NoError(t, err)
	}
	sl2 := []string{}
	p := NewParser(bytes.NewReader(w.Bytes()))
	for i := 0; i < len(sl); i++ {
		s, err := ReadPktLine(p)
		require.NoError(t, err)
		sl2 = append(sl2, s)
	}
	assert.Equal(t, sl, sl2)
}
