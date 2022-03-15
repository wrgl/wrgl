// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package testutils

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func AssertBytesEqual(t *testing.T, a, b [][]byte, ignoreOrder bool) {
	t.Helper()
	if ignoreOrder {
		sl := make([][]byte, len(a))
		copy(sl, a)
		a = sl
		sort.Slice(a, func(i, j int) bool {
			return bytes.Compare(a[i], a[j]) == -1
		})
		sl = make([][]byte, len(b))
		copy(sl, b)
		b = sl
		sort.Slice(b, func(i, j int) bool {
			return bytes.Compare(b[i], b[j]) == -1
		})
	}
	assert.Equal(t, a, b)
}
