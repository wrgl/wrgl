package ref

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	reftestutils "github.com/wrgl/core/pkg/ref/testutils"
)

func TestSeekCommonAncestor(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	base, _ := reftestutils.SaveTestCommit(t, db, nil)
	com1, _ := reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com2, _ := reftestutils.SaveTestCommit(t, db, [][]byte{base})
	b, err := SeekCommonAncestor(db, com1, com2)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	base, _ = reftestutils.SaveTestCommit(t, db, nil)
	com1, _ = reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com2, _ = reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com3, _ := reftestutils.SaveTestCommit(t, db, [][]byte{com2})
	b, err = SeekCommonAncestor(db, com1, com3)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	base, _ = reftestutils.SaveTestCommit(t, db, nil)
	com1, _ = reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com2, _ = reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com3, _ = reftestutils.SaveTestCommit(t, db, [][]byte{com1})
	b, err = SeekCommonAncestor(db, com2, com3)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	com1, _ = reftestutils.SaveTestCommit(t, db, nil)
	com2, _ = reftestutils.SaveTestCommit(t, db, nil)
	_, err = SeekCommonAncestor(db, com1, com2)
	assert.Equal(t, "common ancestor commit not found", err.Error())

	com1, _ = reftestutils.SaveTestCommit(t, db, nil)
	com2, _ = reftestutils.SaveTestCommit(t, db, nil)
	com3, _ = reftestutils.SaveTestCommit(t, db, [][]byte{com1})
	com4, _ := reftestutils.SaveTestCommit(t, db, [][]byte{com2})
	_, err = SeekCommonAncestor(db, com3, com4)
	assert.Equal(t, "common ancestor commit not found", err.Error())

	com1, _ = reftestutils.SaveTestCommit(t, db, nil)
	com2, _ = reftestutils.SaveTestCommit(t, db, nil)
	com3, _ = reftestutils.SaveTestCommit(t, db, [][]byte{com2})
	base, _ = reftestutils.SaveTestCommit(t, db, [][]byte{com1, com3})
	com4, _ = reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com5, _ := reftestutils.SaveTestCommit(t, db, [][]byte{base})
	com6, _ := reftestutils.SaveTestCommit(t, db, [][]byte{com5})
	com7, _ := reftestutils.SaveTestCommit(t, db, [][]byte{com6})
	b, err = SeekCommonAncestor(db, com4, com7)
	require.NoError(t, err)
	assert.Equal(t, base, b)
}
