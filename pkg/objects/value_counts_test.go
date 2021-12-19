package objects

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/misc"
)

func TestSortValueCounts(t *testing.T) {
	sl := ValueCounts{
		{"england", 10},
		{"us", 12},
		{"france", 12},
		{"sydney", 30},
		{"china", 2},
	}
	sort.Sort(sl)
	assert.Equal(t, ValueCounts{
		{"sydney", 30},
		{"france", 12},
		{"us", 12},
		{"england", 10},
		{"china", 2},
	}, sl)
	assert.False(t, sl.IsEmpty())

	sl = ValueCounts{
		{"england", 10},
	}
	sort.Sort(sl)
	assert.Equal(t, ValueCounts{
		{"england", 10},
	}, sl)
	assert.False(t, sl.IsEmpty())

	sl = nil
	sort.Sort(sl)
	assert.Nil(t, sl)
	assert.True(t, sl.IsEmpty())

	sl = ValueCounts{}
	sort.Sort(sl)
	assert.Equal(t, ValueCounts{}, sl)
	assert.False(t, sl.IsEmpty())
}

func TestWriteValueCounts(t *testing.T) {
	for _, sl := range []ValueCounts{
		{
			{"england", 10},
			{"france", 12},
			{"sydney", 30},
			{"china", 2},
		},
		{},
		nil,
	} {
		w := bytes.NewBuffer(nil)
		buf := misc.NewBuffer(nil)
		n, err := writeValueCounts(w, buf, sl)
		require.NoError(t, err)

		p := encoding.NewParser(bytes.NewReader(w.Bytes()))
		a := &ValueCounts{}
		m, err := readValueCounts(p, a)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		if sl == nil {
			assert.Len(t, *a, 0)
		} else {
			assert.Equal(t, sl, *a)
		}
	}
}
