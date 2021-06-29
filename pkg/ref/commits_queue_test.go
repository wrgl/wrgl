// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/objects"
	reftestutils "github.com/wrgl/core/pkg/ref/testutils"
	"github.com/wrgl/core/pkg/testutils"
)

func TestCommitQueue(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	sum1, c1 := reftestutils.SaveTestCommit(t, db, nil)
	sum2, c2 := reftestutils.SaveTestCommit(t, db, nil)
	sum3, c3 := reftestutils.SaveTestCommit(t, db, [][]byte{sum1})
	sum4, c4 := reftestutils.SaveTestCommit(t, db, [][]byte{sum2})
	sum5, c5 := reftestutils.SaveTestCommit(t, db, [][]byte{sum3, sum4})
	sum6, c6 := reftestutils.SaveTestCommit(t, db, [][]byte{sum3})

	q, err := NewCommitsQueue(db, [][]byte{sum5, sum6})
	require.NoError(t, err)
	assert.True(t, q.Seen(sum5))
	assert.True(t, q.Seen(sum6))
	assert.False(t, q.Seen(sum1))

	sum, c, err := q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum6, sum)
	objects.AssertCommitEqual(t, c6, c)
	sum, c, err = q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum5, sum)
	objects.AssertCommitEqual(t, c5, c)
	sum, c, err = q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	objects.AssertCommitEqual(t, c4, c)
	sum, c, err = q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	objects.AssertCommitEqual(t, c3, c)
	sum, c, err = q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	objects.AssertCommitEqual(t, c2, c)
	sum, c, err = q.PopInsertParents()
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	objects.AssertCommitEqual(t, c1, c)
	_, _, err = q.PopInsertParents()
	assert.Equal(t, io.EOF, err)

	assert.True(t, q.Seen(sum2))
	assert.True(t, q.Seen(sum3))
	assert.True(t, q.Seen(sum4))
	assert.True(t, q.Seen(sum5))
}

func TestCommitQueuePopUntil(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	sums := make([][]byte, 0, 5)
	commits := make([]*objects.Commit, 0, 5)
	parents := [][]byte{}
	for i := 0; i < 5; i++ {
		sum, c := reftestutils.SaveTestCommit(t, db, parents)
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
	assert.True(t, q.Seen(sums[1]))
	assert.False(t, q.Seen(sums[0]))

	_, _, err = q.PopUntil(testutils.SecureRandomBytes(16))
	assert.Equal(t, io.EOF, err)
}

func TestCommitQueueRemoveAncestors(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	sum1, _ := reftestutils.SaveTestCommit(t, db, nil)
	sum2, _ := reftestutils.SaveTestCommit(t, db, nil)
	sum3, _ := reftestutils.SaveTestCommit(t, db, nil)
	sum4, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum1})
	sum5, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum2, sum3})
	sum6, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum3})
	sum7, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum4})
	sum8, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum5})
	sum9, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum6})

	q, err := NewCommitsQueue(db, [][]byte{sum9, sum8, sum6, sum5, sum4, sum3, sum2, sum1})
	require.NoError(t, err)
	err = q.RemoveAncestors([][]byte{sum7, sum8})
	require.NoError(t, err)
	sum, _, err := q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum9, sum)
	sum, _, err = q.Pop()
	require.NoError(t, err)
	assert.Equal(t, sum6, sum)
	_, _, err = q.Pop()
	assert.Equal(t, io.EOF, err)
}

func TestIsAncestorOf(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	sum1, _ := reftestutils.SaveTestCommit(t, db, nil)
	sum2, _ := reftestutils.SaveTestCommit(t, db, nil)
	sum3, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum1})
	sum4, _ := reftestutils.SaveTestCommit(t, db, [][]byte{sum3, sum2})
	sum5, _ := reftestutils.SaveTestCommit(t, db, nil)
	ok, err := IsAncestorOf(db, sum4, sum1)
	require.NoError(t, err)
	assert.False(t, ok)
	ok, err = IsAncestorOf(db, sum1, sum4)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = IsAncestorOf(db, sum2, sum4)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = IsAncestorOf(db, sum5, sum4)
	require.NoError(t, err)
	assert.False(t, ok)
}
