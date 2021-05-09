package versioning

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

// createTimeGen create a time generator that returns a timestamp that increase by 1 second
// each time it is called. This ensures that all commits have different timestamp.
func createTimeGen() func() time.Time {
	t := time.Now()
	return func() time.Time {
		t = t.Add(time.Second)
		return t
	}
}

func saveCommit(t *testing.T, db kv.DB, tg func() time.Time, parents [][]byte) (sum []byte, commit *objects.Commit) {
	t.Helper()
	commit = &objects.Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        tg(),
		Message:     testutils.BrokenRandomAlphaNumericString(40),
		Parents:     parents,
	}
	var err error
	sum, err = SaveCommit(db, 0, commit)
	require.NoError(t, err)
	return sum, commit
}

func TestCommitQueue(t *testing.T) {
	db := kv.NewMockStore(false)
	tg := createTimeGen()
	sum1, c1 := saveCommit(t, db, tg, nil)
	sum2, c2 := saveCommit(t, db, tg, nil)
	sum3, c3 := saveCommit(t, db, tg, [][]byte{sum1})
	sum4, c4 := saveCommit(t, db, tg, [][]byte{sum2})
	sum5, c5 := saveCommit(t, db, tg, [][]byte{sum3, sum4})
	sum6, c6 := saveCommit(t, db, tg, [][]byte{sum3})

	q, err := NewCommitsQueue(db, [][]byte{sum5, sum6})
	require.NoError(t, err)
	assert.True(t, q.Seen(sum5))
	assert.True(t, q.Seen(sum6))
	assert.False(t, q.Seen(sum1))

	sum, c, err := q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum6, sum)
	objects.AssertCommitEqual(t, c6, c)
	sum, c, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum5, sum)
	objects.AssertCommitEqual(t, c5, c)
	sum, c, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	objects.AssertCommitEqual(t, c4, c)
	sum, c, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	objects.AssertCommitEqual(t, c3, c)
	sum, c, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	objects.AssertCommitEqual(t, c2, c)
	sum, c, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	objects.AssertCommitEqual(t, c1, c)
	_, _, err = q.Pop()
	assert.Equal(t, io.EOF, err)

	assert.True(t, q.Seen(sum2))
	assert.True(t, q.Seen(sum3))
	assert.True(t, q.Seen(sum4))
	assert.True(t, q.Seen(sum5))
}

func TestCommitQueuePopUntil(t *testing.T) {
	db := kv.NewMockStore(false)
	tg := createTimeGen()
	sums := make([][]byte, 0, 5)
	commits := make([]*objects.Commit, 0, 5)
	parents := [][]byte{}
	for i := 0; i < 5; i++ {
		sum, c := saveCommit(t, db, tg, parents)
		sums = append(sums, sum)
		commits = append(commits, c)
		parents = [][]byte{sum}
	}

	q, err := NewCommitsQueue(db, [][]byte{sums[4]})
	require.NoError(t, err)
	sum, c, err := q.PopUntil(sums[2])
	require.NoError(t, err)
	assert.Equal(t, sums[2], sum)
	objects.AssertCommitEqual(t, commits[2], c)
	assert.True(t, q.Seen(sums[4]))
	assert.True(t, q.Seen(sums[3]))
	assert.True(t, q.Seen(sums[2]))
	assert.False(t, q.Seen(sums[1]))
	assert.False(t, q.Seen(sums[0]))

	_, _, err = q.PopUntil(testutils.SecureRandomBytes(16))
	assert.Equal(t, io.EOF, err)
}
