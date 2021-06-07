package merge

import (
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func randomStrSlice(n int) []string {
	sl := make([]string, n)
	for i := range sl {
		k := 3 + rand.Intn(4)
		sl[i] = testutils.BrokenRandomAlphaNumericString(k)
	}
	return sl
}

func collectRows(t *testing.T, sr *SortableRows) [][]string {
	t.Helper()
	errCh := make(chan error, 10)
	ch := sr.RowsChan(errCh)
	sl := [][]string{}
	for row := range ch {
		sl = append(sl, row)
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return sl
}

func sortByColumns(sl [][]string, sortBy []int) {
	sort.Slice(sl, func(i, j int) bool {
		for _, c := range sortBy {
			k := c - 1
			if c < 0 {
				k = -c - 1
			}
			if sl[i][k] < sl[j][k] {
				return c > 0
			} else if sl[i][k] > sl[j][k] {
				return c < 0
			}
		}
		return false
	})
}

func TestSortableRows(t *testing.T) {
	rows, err := ioutil.TempFile("", "test_rows")
	require.NoError(t, err)
	defer os.Remove(rows.Name())
	offsets, err := ioutil.TempFile("", "test_offsets")
	require.NoError(t, err)
	defer os.Remove(offsets.Name())
	sl := [][]string{}

	sr, err := NewSortableRows(rows, offsets, []int{1, 2})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		sl = append(sl, randomStrSlice(5))
		require.NoError(t, sr.Add(sl[i]))
	}
	assert.Equal(t, 3, sr.Len())
	sort.Sort(sr)
	sl2 := collectRows(t, sr)
	sortByColumns(sl, []int{1, 2})
	assert.Equal(t, sl, sl2)
	require.NoError(t, sr.Close())

	rows, err = os.OpenFile(rows.Name(), os.O_RDWR, 0666)
	require.NoError(t, err)
	offsets, err = os.OpenFile(offsets.Name(), os.O_RDWR, 0666)
	require.NoError(t, err)
	sortBy := []int{-3, -4}
	sr, err = NewSortableRows(rows, offsets, sortBy)
	require.NoError(t, err)
	for i := 0; i < 1024; i++ {
		sl = append(sl, randomStrSlice(5))
		require.NoError(t, sr.Add(sl[i+3]))
	}
	assert.Equal(t, 1027, sr.Len())
	sort.Sort(sr)
	sl2 = collectRows(t, sr)
	sortByColumns(sl, sortBy)
	assert.Equal(t, sl, sl2)
	require.NoError(t, sr.Close())
}
