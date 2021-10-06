package ref_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
)

func TestSeekCommonAncestor(t *testing.T) {
	db := objmock.NewStore()

	base, _ := refhelpers.SaveTestCommit(t, db, nil)
	com1, _ := refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com2, _ := refhelpers.SaveTestCommit(t, db, [][]byte{base})
	b, err := ref.SeekCommonAncestor(db, com1, com2)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	base, _ = refhelpers.SaveTestCommit(t, db, nil)
	com1, _ = refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com2, _ = refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com3, _ := refhelpers.SaveTestCommit(t, db, [][]byte{com2})
	b, err = ref.SeekCommonAncestor(db, com1, com3)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	base, _ = refhelpers.SaveTestCommit(t, db, nil)
	com1, _ = refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com2, _ = refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com3, _ = refhelpers.SaveTestCommit(t, db, [][]byte{com1})
	b, err = ref.SeekCommonAncestor(db, com2, com3)
	require.NoError(t, err)
	assert.Equal(t, base, b)

	com1, _ = refhelpers.SaveTestCommit(t, db, nil)
	com2, _ = refhelpers.SaveTestCommit(t, db, nil)
	_, err = ref.SeekCommonAncestor(db, com1, com2)
	assert.Equal(t, "common ancestor commit not found", err.Error())

	com1, _ = refhelpers.SaveTestCommit(t, db, nil)
	com2, _ = refhelpers.SaveTestCommit(t, db, nil)
	com3, _ = refhelpers.SaveTestCommit(t, db, [][]byte{com1})
	com4, _ := refhelpers.SaveTestCommit(t, db, [][]byte{com2})
	_, err = ref.SeekCommonAncestor(db, com3, com4)
	assert.Equal(t, "common ancestor commit not found", err.Error())

	com1, _ = refhelpers.SaveTestCommit(t, db, nil)
	com2, _ = refhelpers.SaveTestCommit(t, db, nil)
	com3, _ = refhelpers.SaveTestCommit(t, db, [][]byte{com2})
	base, _ = refhelpers.SaveTestCommit(t, db, [][]byte{com1, com3})
	com4, _ = refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com5, _ := refhelpers.SaveTestCommit(t, db, [][]byte{base})
	com6, _ := refhelpers.SaveTestCommit(t, db, [][]byte{com5})
	com7, _ := refhelpers.SaveTestCommit(t, db, [][]byte{com6})
	b, err = ref.SeekCommonAncestor(db, com4, com7)
	require.NoError(t, err)
	assert.Equal(t, base, b)
}
