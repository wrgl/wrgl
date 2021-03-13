package encoding

import (
	"math/rand"
	"testing"

	"github.com/wrgl/core/pkg/testutils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkStringsEncode(b *testing.B) {
	keysList := [][]string{}
	for i := 0; i < b.N; i++ {
		keyLen := rand.Intn(3) + 1
		keys := []string{}
		for j := 0; j < keyLen; j++ {
			keys = append(keys, testutils.BrokenRandomAlphaNumericString(rand.Intn(11)+1))
		}
		keysList = append(keysList, keys)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := EncodeStrings(keysList[i])
		require.NoError(b, err)
	}
}

func TestBatchDecodeStrings(t *testing.T) {
	rows := [][]string{
		{"", ""},
		{"1", `"2`},
		{"", "b,"},
		{",a", ""},
		{"4", `3"`},
	}
	m := [][]byte{}
	for _, r := range rows {
		b, err := EncodeStrings(r)
		require.NoError(t, err)
		s, err := DecodeStrings(b)
		require.NoError(t, err)
		assert.Equal(t, r, s)
		m = append(m, b)
	}
	m2, err := BatchDecodeStrings(m)
	require.NoError(t, err)
	assert.Equal(t, rows, m2)
}
