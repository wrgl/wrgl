package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repeatUintSlice(sl []uint32, n int) []uint32 {
	l := len(sl)
	result := make([]uint32, n*l)
	for i := 0; i < n; i++ {
		for j, s := range sl {
			result[i*l+j] = s
		}
	}
	return result
}

func TestUintListEncoder(t *testing.T) {
	e := NewUintListEncoder()
	d := NewUintListDecoder(false)
	slices := [][]uint32{
		{},
		{0},
		{10, 20, 30},
		repeatUintSlice([]uint32{11, 22, 33, 44}, 128),
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

func TestUintListDecoderReuseRecords(t *testing.T) {
	b1 := NewUintListEncoder().Encode([]uint32{1, 2})
	b2 := NewUintListEncoder().Encode([]uint32{3, 4, 5})

	d := NewUintListDecoder(false)
	sl1 := d.Decode(b1)
	sl2 := d.Decode(b2)
	assert.Equal(t, []uint32{1, 2}, sl1)
	assert.Equal(t, []uint32{3, 4, 5}, sl2)

	d = NewUintListDecoder(true)
	sl1 = d.Decode(b1)
	sl2 = d.Decode(b2)
	assert.Equal(t, []uint32{3, 4}, sl1)
	assert.Equal(t, []uint32{3, 4, 5}, sl2)
}
