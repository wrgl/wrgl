// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestTraveller(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()

	_, err := ref.NewTraveller(db, rs, "non-existent")
	assert.Equal(t, ref.ErrKeyNotFound, err)

	sum1, c1 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1, nil))
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2, nil))
	sum3, _ := factory.CommitRandom(t, db, nil)
	sum4, c4 := factory.CommitRandom(t, db, [][]byte{sum2, sum3})
	require.NoError(t, ref.CommitMerge(rs, "main", sum4, c4))

	tr, err := ref.NewTraveller(db, rs, "heads/main")
	require.NoError(t, err)
	defer tr.Close()
	com, err := tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c4, com)
	assert.Equal(t, "merge", tr.Reflog.Action)
	com, err = tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c2, com)
	assert.Equal(t, "commit", tr.Reflog.Action)
	assert.Equal(t, sum2, tr.Reflog.NewOID)
	com, err = tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c1, com)
	assert.Equal(t, "commit", tr.Reflog.Action)
	assert.Equal(t, sum1, tr.Reflog.NewOID)
	com, err = tr.Next()
	require.NoError(t, err)
	assert.Nil(t, com)
	com, err = tr.Next()
	require.NoError(t, err)
	assert.Nil(t, com)

	sum5, c5 := factory.CommitRandom(t, db, nil)
	sum6, c6 := factory.CommitRandom(t, db, [][]byte{sum5})
	require.NoError(t, ref.SaveFetchRef(rs, "heads/alpha", sum6, "John Doe", "john@doe.com", "origin", "storing head"))

	tr, err = ref.NewTraveller(db, rs, "heads/alpha")
	require.NoError(t, err)
	defer tr.Close()
	com, err = tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c6, com)
	assert.Equal(t, "fetch", tr.Reflog.Action)
	com, err = tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c5, com)
	assert.Equal(t, "fetch", tr.Reflog.Action)
	com, err = tr.Next()
	require.NoError(t, err)
	assert.Nil(t, com)

	sum7, _ := factory.CommitRandom(t, db, nil)
	sum7, c7 := factory.CommitRandom(t, db, [][]byte{sum7})
	require.NoError(t, ref.SaveRef(rs, "heads/gamma", sum7, "John Doe", "john@doe.com", "branch", "create new", nil))

	tr, err = ref.NewTraveller(db, rs, "heads/gamma")
	require.NoError(t, err)
	defer tr.Close()
	com, err = tr.Next()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, c7, com)
	assert.Equal(t, "branch", tr.Reflog.Action)
	com, err = tr.Next()
	require.NoError(t, err)
	assert.Nil(t, com)
}
