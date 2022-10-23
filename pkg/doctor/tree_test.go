package doctor

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestTree(t *testing.T) {
	db := objmock.NewStore()
	sum1, com1 := factory.CommitRandom(t, db, nil)
	sum2, com2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, com3 := factory.CommitRandom(t, db, [][]byte{sum2})

	tree := NewTree(db)
	require.NoError(t, tree.Reset(sum3))

	com, err := tree.Up()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com3, com)

	com, err = tree.Up()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com2, com)

	com, err = tree.Up()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com1, com)

	_, err = tree.Up()
	assert.Equal(t, io.EOF, err)

	com, err = tree.Down()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com1, com)

	com, err = tree.Down()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com2, com)

	com, err = tree.Down()
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com3, com)

	_, err = tree.Down()
	assert.Equal(t, io.EOF, err)

	ancestors, descendants, err := tree.Position(sum1)
	require.NoError(t, err)
	assert.Equal(t, 0, ancestors)
	assert.Equal(t, 2, descendants)

	ancestors, descendants, err = tree.Position(sum2)
	require.NoError(t, err)
	assert.Equal(t, 1, ancestors)
	assert.Equal(t, 1, descendants)

	ancestors, descendants, err = tree.Position(sum3)
	require.NoError(t, err)
	assert.Equal(t, 2, ancestors)
	assert.Equal(t, 0, descendants)

	_, _, err = tree.Position(testutils.SecureRandomBytes(16))
	assert.Error(t, err)
}

func upAllTheWay(tree *Tree) {
	for {
		_, err := tree.Up()
		if err != nil {
			break
		}
	}
}

func TestTree_EditCommit(t *testing.T) {
	db := objmock.NewStore()
	sum1, _ := factory.CommitRandom(t, db, nil)
	sum2, com2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, com3 := factory.CommitRandom(t, db, [][]byte{sum2})

	tree := NewTree(db)
	require.NoError(t, tree.Reset(sum3))
	upAllTheWay(tree)

	require.NoError(t, tree.EditCommit(sum2, func(com *objects.Commit) (remove bool, update bool, err error) {
		com.AuthorName = "John Doe"
		update = true
		return
	}))
	sum, err := tree.UpdateAllDescendants()
	require.NoError(t, err)
	com := factory.GetCommit(t, db, sum)
	assert.Equal(t, com3.Table, com.Table)
	assert.NotEqual(t, com3.Parents, com.Parents)

	com = factory.GetParent(t, db, com)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, com2.Table, com.Table)
	assert.Equal(t, com2.Parents, com.Parents)
}

func TestTree_EditCommit_remove(t *testing.T) {
	db := objmock.NewStore()
	sum1, _ := factory.CommitRandom(t, db, nil)
	sum2, _ := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, com3 := factory.CommitRandom(t, db, [][]byte{sum2})

	tree := NewTree(db)
	require.NoError(t, tree.Reset(sum3))
	upAllTheWay(tree)

	require.NoError(t, tree.EditCommit(sum2, func(com *objects.Commit) (remove bool, update bool, err error) {
		remove = true
		return
	}))
	sum, err := tree.UpdateAllDescendants()
	require.NoError(t, err)
	com := factory.GetCommit(t, db, sum)
	assert.Equal(t, com3.Table, com.Table)
	assert.Len(t, com.Parents, 0)
}
