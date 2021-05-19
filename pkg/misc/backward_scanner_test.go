// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package misc

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestBackwardScanner(t *testing.T) {
	s1 := testutils.BrokenRandomAlphaNumericString(2000)
	strs := []string{
		"abc",
		"123",
		s1,
		"def",
	}
	reader := bytes.NewReader([]byte(strings.Join(strs, "\n")))
	scanner, err := NewBackwardScanner(reader)
	require.NoError(t, err)
	for i := 3; i >= 0; i-- {
		s, err := scanner.ReadLine()
		require.NoError(t, err)
		assert.Equal(t, strs[i], s)
	}
	_, err = scanner.ReadLine()
	assert.Equal(t, io.EOF, err)
}
