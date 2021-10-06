package reffs

import (
	"io"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestStore(t *testing.T) {
	dir, err := testutils.TempDir("", "refstore")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	s := NewStore(dir)

	// test Set & Get
	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, s.Set("tags/abc", sum))
	b, err := s.Get("tags/abc")
	require.NoError(t, err)
	assert.Equal(t, sum, b)

	// test Delete
	require.NoError(t, s.Delete("tags/abc"))
	_, err = s.Get("tags/abc")
	assert.Equal(t, ref.ErrKeyNotFound, err)

	// test SetWithLog & LogReader
	sum2 := testutils.SecureRandomBytes(16)
	l2 := refhelpers.RandomReflog()
	require.NoError(t, s.SetWithLog("heads/alpha", sum2, l2))
	b, err = s.Get("heads/alpha")
	require.NoError(t, err)
	assert.Equal(t, sum2, b)
	sum3 := testutils.SecureRandomBytes(16)
	l3 := refhelpers.RandomReflog()
	require.NoError(t, s.SetWithLog("heads/alpha", sum3, l3))
	b, err = s.Get("heads/alpha")
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	r, err := s.LogReader("heads/alpha")
	require.NoError(t, err)
	l, err := r.Read()
	require.NoError(t, err)
	refhelpers.AssertReflogEqual(t, l3, l)
	l, err = r.Read()
	require.NoError(t, err)
	refhelpers.AssertReflogEqual(t, l2, l)
	_, err = r.Read()
	assert.Equal(t, io.EOF, err)

	// test Rename
	require.NoError(t, s.Rename("heads/alpha", "heads/beta"))
	_, err = s.Get("heads/alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	_, err = s.LogReader("heads/alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	b, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/beta", l3)

	// test Copy
	require.NoError(t, s.Copy("heads/beta", "heads/theta"))
	b, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/beta", l3)
	b, err = s.Get("heads/theta")
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/theta", l3)

	// test Delete along with logs
	require.NoError(t, s.Delete("heads/beta"))
	_, err = s.Get("heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	_, err = s.LogReader("heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)

	// test FilterKey
	sum4 := testutils.SecureRandomBytes(16)
	l4 := refhelpers.RandomReflog()
	require.NoError(t, s.SetWithLog("heads/gamma", sum4, l4))
	sl, err := s.FilterKey("heads/")
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	assert.Equal(t, []string{
		"heads/gamma",
		"heads/theta",
	}, sl)

	sum5 := testutils.SecureRandomBytes(16)
	require.NoError(t, s.Set("tags/def", sum5))
	sl, err = s.FilterKey("")
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	assert.Equal(t, []string{
		"heads/gamma",
		"heads/theta",
		"tags/def",
	}, sl)

	// test Filter
	m, err := s.Filter("heads/")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/gamma": sum4,
		"heads/theta": sum3,
	}, m)
	m, err = s.Filter("")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/gamma": sum4,
		"heads/theta": sum3,
		"tags/def":    sum5,
	}, m)

	// Filter non-existent keys
	sl, err = s.FilterKey("remotes/")
	require.NoError(t, err)
	assert.Len(t, sl, 0)
	m, err = s.Filter("remotes/")
	require.NoError(t, err)
	assert.Len(t, m, 0)
}
