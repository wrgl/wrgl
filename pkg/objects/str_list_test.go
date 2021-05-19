// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repeatStrSlice(sl []string, n int) []string {
	l := len(sl)
	result := make([]string, n*l)
	for i := 0; i < n; i++ {
		for j, s := range sl {
			result[i*l+j] = s
		}
	}
	return result
}

func TestEncodeStrList(t *testing.T) {
	e := NewStrListEncoder()
	d := NewStrListDecoder(false)
	slices := [][]string{
		{"a", "b", "c"},
		{"a"},
		{},
		{"chữ", "tiếng", "Việt", "にほんご", "汉字"},
		{"", "a", "", "b", "", "", "c", ""},
		repeatStrSlice([]string{"aa", "bb", "cc", "dd"}, 128),
	}

	// test Encode & Decode
	for _, sl := range slices {
		b := e.Encode(sl)
		assert.Equal(t, sl, d.Decode(b))
	}

	// test Read
	buf := bytes.NewBufferString("")
	for _, sl := range slices {
		_, err := buf.Write(e.Encode(sl))
		require.NoError(t, err)
	}
	for i := 0; i < len(slices); i++ {
		n, sl, err := d.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, slices[i], sl)
		assert.NotEmpty(t, n)
	}
}

func TestStrListDecoderReuseRecords(t *testing.T) {
	b1 := NewStrListEncoder().Encode([]string{"a", "b"})
	b2 := NewStrListEncoder().Encode([]string{"c", "d", "e"})

	d := NewStrListDecoder(false)
	sl1 := d.Decode(b1)
	sl2 := d.Decode(b2)
	assert.Equal(t, []string{"a", "b"}, sl1)
	assert.Equal(t, []string{"c", "d", "e"}, sl2)

	d = NewStrListDecoder(true)
	sl1 = d.Decode(b1)
	sl2 = d.Decode(b2)
	assert.Equal(t, []string{"c", "d"}, sl1)
	assert.Equal(t, []string{"c", "d", "e"}, sl2)
}
