// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repeatFloatSlice(sl []float64, n int) []float64 {
	l := len(sl)
	result := make([]float64, n*l)
	for i := 0; i < n; i++ {
		for j, s := range sl {
			result[i*l+j] = s
		}
	}
	return result
}

func TestFloatListEncoder(t *testing.T) {
	e := NewFloatListEncoder()
	d := NewFloatListDecoder(false)
	slices := [][]float64{
		{},
		{0},
		{10.1, 20.2, 30.3},
		repeatFloatSlice([]float64{-11, 22, -33, 44}, 128),
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

func TestFloatListDecoderReuseRecords(t *testing.T) {
	b1 := NewFloatListEncoder().Encode([]float64{1.2, 2.3})
	b2 := NewFloatListEncoder().Encode([]float64{3, -4, 5})

	d := NewFloatListDecoder(false)
	sl1 := d.Decode(b1)
	sl2 := d.Decode(b2)
	assert.Equal(t, []float64{1.2, 2.3}, sl1)
	assert.Equal(t, []float64{3, -4, 5}, sl2)

	d = NewFloatListDecoder(true)
	sl1 = d.Decode(b1)
	sl2 = d.Decode(b2)
	assert.Equal(t, []float64{3, -4}, sl1)
	assert.Equal(t, []float64{3, -4, 5}, sl2)
}
