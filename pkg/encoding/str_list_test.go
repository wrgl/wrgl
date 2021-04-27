package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	for _, sl := range [][]string{
		{"a", "b", "c"},
		{"a"},
		{},
		{"chữ", "tiếng", "Việt", "にほんご", "汉字"},
		repeatStrSlice([]string{"aa", "bb", "cc", "dd"}, 128),
	} {
		b := e.Encode(sl)
		assert.Equal(t, sl, d.Decode(b))
	}
}

func TestReuseRecords(t *testing.T) {
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
