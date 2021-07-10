// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
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
	e := NewStrListEncoder(true)
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
	r := bytes.NewReader(buf.Bytes())
	for i := 0; i < len(slices); i++ {
		n, sl, err := d.Read(r)
		require.NoError(t, err)
		assert.Equal(t, slices[i], sl)
		assert.NotEmpty(t, n)
	}

	// test ReadBytes
	r = bytes.NewReader(buf.Bytes())
	for i := 0; ; i++ {
		n, b, err := d.ReadBytes(r)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.Len(t, b, int(n))
		assert.Equal(t, slices[i], d.Decode(b))
	}
}

func TestStrListDecoderReuseRecords(t *testing.T) {
	enc := NewStrListEncoder(false)
	b1 := enc.Encode([]string{"a", "b"})
	b2 := enc.Encode([]string{"c", "d", "e"})

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

func TestReadColumnsFromStrListBytes(t *testing.T) {
	enc := NewStrListEncoder(true)
	row := make([]string, 10)
	for i := range row {
		row[i] = testutils.BrokenRandomAlphaNumericString(10)
	}
	b := StrList(enc.Encode(row))

	assert.Equal(t, []string{row[0]}, b.ReadColumns([]uint32{0}))
	assert.Equal(t, []string{row[1], row[2]}, b.ReadColumns([]uint32{1, 2}))
	assert.Equal(t, []string{row[5], row[3]}, b.ReadColumns([]uint32{5, 3}))
}

func TestStrListBytesLess(t *testing.T) {
	rows := testutils.BuildRawCSV(10, 100)
	rowBytes := make([][]byte, len(rows))
	enc := NewStrListEncoder(false)
	for i, row := range rows {
		rowBytes[i] = enc.Encode(row)
	}
	dec := NewStrListDecoder(true)
	for _, cols := range [][]uint32{{1}, {2, 3}, {5, 4}} {
		sort.Slice(rows, func(i, j int) bool {
			for _, u := range cols {
				if rows[i][u] < rows[j][u] {
					return true
				} else if rows[i][u] > rows[j][u] {
					return false
				}
			}
			return false
		})
		sort.Slice(rowBytes, func(i, j int) bool {
			return StrList(rowBytes[i]).LessThan(cols, rowBytes[j])
		})
		for i, b := range rowBytes {
			assert.Equal(t, rows[i], dec.Decode(b))
		}
	}
}

func TestRemoveColumnsFromStrList(t *testing.T) {
	row := make([]string, 10)
	for i := range row {
		row[i] = testutils.BrokenRandomAlphaNumericString(10)
	}
	b := NewStrListEncoder(true).Encode(row)
	dec := NewStrListDecoder(true)

	row = append(row[:1], row[2:]...)
	r := NewColumnRemover(map[int]struct{}{1: {}})
	b = r.RemoveFrom(b)
	assert.Equal(t, row, dec.Decode(b))

	row = append(row[:3], row[4:]...)
	row = append(row[:2], row[3:]...)
	r = NewColumnRemover(map[int]struct{}{2: {}, 3: {}})
	b = r.RemoveFrom(b)
	assert.Equal(t, row, dec.Decode(b))

	row = append(row[:5], row[6:]...)
	row = append(row[:4], row[5:]...)
	r = NewColumnRemover(map[int]struct{}{5: {}, 4: {}})
	b = r.RemoveFrom(b)
	assert.Equal(t, row, dec.Decode(b))
}

func BenchmarkStrListDecoder(b *testing.B) {
	enc := NewStrListEncoder(true)
	dec := NewStrListDecoder(true)
	rows := testutils.BuildRawCSV(10, b.N)
	b.ResetTimer()
	var sl []byte
	for _, row := range rows {
		sl = enc.Encode(row)
		dec.Decode(sl)
	}
}
