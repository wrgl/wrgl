// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestTransaction(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	id, err := rs.NewTransaction(nil)
	require.NoError(t, err)

	sum1, com1 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveTransactionRef(rs, *id, "alpha", sum1))

	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	sum3, com3 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveTransactionRef(rs, *id, "beta", sum3))

	m, tx, err := Diff(rs, *id)
	require.NoError(t, err)
	assert.Equal(t, map[string][2][]byte{
		"alpha": {sum1, nil},
		"beta":  {sum3, sum2},
	}, m)
	assert.Equal(t, ref.TSInProgress, tx.Status)

	commits, err := Commit(db, rs, *id)
	require.NoError(t, err)
	assert.Len(t, commits, 2)
	assert.Equal(t, sum1, commits["heads/alpha"].Sum)
	assert.Equal(t, sum3, commits["heads/beta"].Sum)

	sum4, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.NotEqual(t, sum1, sum4)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		NewOID:      sum4,
		AuthorName:  com1.AuthorName,
		AuthorEmail: com1.AuthorEmail,
		Action:      "commit",
		Message:     com1.Message,
		Txid:        id,
	})
	com, err := objects.GetCommit(db, sum4)
	require.NoError(t, err)
	assert.Equal(t, com1.Table, com.Table)
	assert.Equal(t, fmt.Sprintf("commit [tx/%s]\n%s", id, com1.Message), com.Message)

	sum5, err := ref.GetHead(rs, "beta")
	require.NoError(t, err)
	assert.NotEqual(t, sum3, sum5)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/beta", &ref.Reflog{
		NewOID:      sum5,
		OldOID:      sum2,
		AuthorName:  com3.AuthorName,
		AuthorEmail: com3.AuthorEmail,
		Action:      "commit",
		Message:     com3.Message,
		Txid:        id,
	})
	com, err = objects.GetCommit(db, sum5)
	require.NoError(t, err)
	assert.Equal(t, com3.Table, com.Table)
	assert.Equal(t, [][]byte{sum2}, com.Parents)
	assert.Equal(t, fmt.Sprintf("commit [tx/%s]\n%s", id, com3.Message), com.Message)

	tx, err = rs.GetTransaction(*id)
	require.NoError(t, err)
	assert.NotEmpty(t, tx.End)
	assert.Equal(t, ref.TSCommitted, tx.Status)

	m, tx2, err := Diff(rs, *id)
	require.NoError(t, err)
	assert.Equal(t, map[string][2][]byte{
		"alpha": {sum4, nil},
		"beta":  {sum5, sum2},
	}, m)
	assert.Equal(t, tx, tx2)

	// test reapply
	sum6, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, Reapply(db, rs, *id, func(branch string, sum []byte, message string) {
		switch branch {
		case "alpha":
			assert.NotEmpty(t, sum)
			assert.Equal(t, fmt.Sprintf("reapply [tx/%s]\ncommit [tx/%s]\n%s", *id, *id, com1.Message), message)
		case "beta":
			assert.Nil(t, sum)
			assert.Empty(t, message)
		default:
			t.Errorf("unexepected branch %q", branch)
		}
	}))
	sum, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	com, err = objects.GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, com1.Table, com.Table)
	assert.Equal(t, [][]byte{sum6}, com.Parents)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		OldOID:      sum6,
		NewOID:      sum,
		AuthorName:  com1.AuthorName,
		AuthorEmail: com1.AuthorEmail,
		Action:      "reapply",
		Message:     fmt.Sprintf("transaction %s", *id),
	})
	sum, err = ref.GetHead(rs, "beta")
	require.NoError(t, err)
	assert.NotEqual(t, sum3, sum)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/beta", &ref.Reflog{
		NewOID:      sum,
		OldOID:      sum2,
		AuthorName:  com3.AuthorName,
		AuthorEmail: com3.AuthorEmail,
		Action:      "commit",
		Message:     com3.Message,
		Txid:        id,
	})

}

func TestDiscard(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	id, err := rs.NewTransaction(nil)
	require.NoError(t, err)

	sum1, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveTransactionRef(rs, *id, "alpha", sum1))

	require.NoError(t, Discard(rs, *id))
	refs, err := ref.ListTransactionRefs(rs, *id)
	require.NoError(t, err)
	assert.Len(t, refs, 0)
	_, err = rs.GetTransaction(*id)
	assert.Error(t, err)
}

func TestGarbageCollect(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()

	id1, err := rs.NewTransaction(nil)
	require.NoError(t, err)
	id2, err := rs.NewTransaction(nil)
	require.NoError(t, err)

	time.Sleep(time.Second)
	id3, err := rs.NewTransaction(nil)
	require.NoError(t, err)
	require.NoError(t, GarbageCollect(db, rs, time.Second, nil))

	_, err = rs.GetTransaction(*id1)
	assert.Error(t, err)
	_, err = rs.GetTransaction(*id2)
	assert.Error(t, err)
	_, err = rs.GetTransaction(*id3)
	require.NoError(t, err)
}
