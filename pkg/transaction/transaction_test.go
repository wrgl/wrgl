package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestTransaction(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	id, err := New(db)
	require.NoError(t, err)

	sum1, com1 := factory.CommitRandom(t, db, nil)
	require.NoError(t, Add(rs, id, "alpha", sum1))

	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	sum3, com3 := factory.CommitRandom(t, db, nil)
	require.NoError(t, Add(rs, id, "beta", sum3))

	m, err := Diff(rs, id)
	require.NoError(t, err)
	assert.Equal(t, map[string][2][]byte{
		"alpha": {sum1, nil},
		"beta":  {sum3, sum2},
	}, m)

	require.NoError(t, Commit(db, rs, id))
	require.NoError(t, Discard(db, rs, id))

	sum, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  com1.AuthorName,
		AuthorEmail: com1.AuthorEmail,
		Action:      "commit",
		Message:     com1.Message,
	})
	com, err := objects.GetCommit(db, sum)
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com1, com)

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
	})
	com, err = objects.GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, com3.Table, com.Table)
	assert.Equal(t, [][]byte{sum2}, com.Parents)

	refs, err := ref.ListTransactionRefs(rs, id)
	require.NoError(t, err)
	assert.Len(t, refs, 0)
	assert.False(t, objects.TransactionExist(db, id))
}
