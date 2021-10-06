// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package index

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func makeBytesMatrix(n, m int) [][]byte {
	rows := make([][]byte, n)
	for i := 0; i < len(rows); i++ {
		rows[i] = testutils.SecureRandomBytes(m)
	}
	return rows
}

func TestOrderedHashSet(t *testing.T) {
	otherHashes := makeBytesMatrix(10, 16)
	for c, rows := range [][][]byte{
		{},
		{testutils.SecureRandomBytes(32)},
		{testutils.SecureRandomBytes(32), testutils.SecureRandomBytes(32)},
		{
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
		},
		{
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
		},
		makeBytesMatrix(1024, 32),
	} {
		buf := bytes.NewBuffer(nil)
		w := NewOrderedHashSetWriter(buf, rows)
		require.NoError(t, w.Flush())

		i, err := NewOrderedHashSet(objects.NopCloser(bytes.NewReader(buf.Bytes())))
		require.NoError(t, err)
		for j, row := range rows {
			k, err := i.IndexOf(row[:16])
			require.NoError(t, err, "case %d", c)
			assert.Equal(t, j, k, "case %d", c)
		}
		for _, s := range otherHashes {
			k, err := i.IndexOf(s)
			require.NoError(t, err, "case %d", c)
			assert.Equal(t, -1, k, "case %d", c)
		}
	}
}

func BenchmarkOrderedHashSet1M(b *testing.B) {
	n := 1024 * 1024
	rows := makeBytesMatrix(n, 32)
	f, err := ioutil.TempFile("", "test_hash_index")
	require.NoError(b, err)
	defer os.Remove(f.Name())
	w := NewOrderedHashSetWriter(f, rows)
	require.NoError(b, w.Flush())
	require.NoError(b, f.Close())

	f, err = os.Open(f.Name())
	require.NoError(b, err)
	defer f.Close()
	i, err := NewOrderedHashSet(f)
	require.NoError(b, err)
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		k, err := i.IndexOf(rows[j%n])
		require.NoError(b, err)
		assert.Equal(b, j%n, k)
	}
}
