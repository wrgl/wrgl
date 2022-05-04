// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiutils_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestClosedSetsFinder(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, c1 := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, nil)
	sum3, c3 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum4, c4 := factory.CommitRandom(t, db, [][]byte{sum2})
	sum5, c5 := factory.CommitRandom(t, db, [][]byte{sum3})
	sum6, c6 := factory.CommitRandom(t, db, [][]byte{sum4})
	require.NoError(t, ref.CommitHead(rs, "main", sum5, c5, nil))
	require.NoError(t, ref.SaveTag(rs, "v1", sum6))

	// send everything if haves are empty
	finder := apiutils.NewClosedSetsFinder(db, rs, 0)
	acks, err := finder.Process([][]byte{sum5, sum6}, nil, false)
	require.NoError(t, err)
	assert.Empty(t, acks)
	assert.Equal(t, [][]byte{}, finder.CommonCommmits())
	commits, err := finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c2, c1, c3, c4, c5, c6}, commits, true)
	tables, err := finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c2.Table): {},
		string(c1.Table): {},
		string(c3.Table): {},
		string(c4.Table): {},
		string(c5.Table): {},
		string(c6.Table): {},
	}, tables, true)

	// send only necessary commits
	finder = apiutils.NewClosedSetsFinder(db, rs, 0)
	acks, err = finder.Process([][]byte{sum3, sum4}, [][]byte{sum1, sum2}, false)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum1, sum2}, acks)
	testutils.AssertBytesEqual(t, [][]byte{sum1, sum2}, finder.CommonCommmits(), true)
	commits, err = finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c3, c4}, commits, true)
	tables, err = finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c3.Table): {},
		string(c4.Table): {},
	}, tables, true)
}

func TestClosedSetsFinderACKs(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, _ := factory.CommitRandom(t, db, nil)
	sum2, _ := factory.CommitRandom(t, db, nil)
	sum3, _ := factory.CommitRandom(t, db, nil)
	sum4, c4 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum5, c5 := factory.CommitRandom(t, db, [][]byte{sum2, sum3})
	sum6, c6 := factory.CommitRandom(t, db, [][]byte{sum4})
	sum7, c7 := factory.CommitRandom(t, db, [][]byte{sum5})
	sum8, c8 := factory.CommitRandom(t, db, nil)
	sum9, c9 := factory.CommitRandom(t, db, [][]byte{sum8})
	require.NoError(t, ref.CommitHead(rs, "alpha", sum6, c6, nil))
	require.NoError(t, ref.SaveTag(rs, "v1", sum7))
	require.NoError(t, ref.CommitHead(rs, "beta", sum9, c9, nil))

	finder := apiutils.NewClosedSetsFinder(db, rs, 0)
	acks, err := finder.Process([][]byte{sum6, sum7, sum9}, [][]byte{sum1}, false)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum1}, acks)

	acks, err = finder.Process(nil, [][]byte{sum2}, false)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum2}, acks)

	acks, err = finder.Process(nil, [][]byte{sum3}, true)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum3}, acks)

	commits, err := finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c4, c5, c6, c7, c8, c9}, commits, true)
	tables, err := finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c4.Table): {},
		string(c5.Table): {},
		string(c6.Table): {},
		string(c7.Table): {},
		string(c8.Table): {},
		string(c9.Table): {},
	}, tables, true)
}

func TestClosedSetsFinderUnrecognizedWants(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, _ := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2, nil))
	finder := apiutils.NewClosedSetsFinder(db, rs, 0)

	sum3, _ := refhelpers.SaveTestCommit(t, db, [][]byte{sum2})
	_, err := finder.Process([][]byte{sum3}, [][]byte{sum2}, false)
	assert.Error(t, err, "unrecognized wants: "+hex.EncodeToString(sum3))

	sum4 := testutils.SecureRandomBytes(16)
	_, err = finder.Process([][]byte{sum4}, [][]byte{sum1}, false)
	assert.Error(t, err, "unrecognized wants: "+hex.EncodeToString(sum4))
}

func TestClosedSetsFinderDepth(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, c1 := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	sum3, c3 := factory.CommitRandom(t, db, [][]byte{sum2})
	sum4, c4 := factory.CommitRandom(t, db, [][]byte{sum3})
	sum5, c5 := factory.CommitRandom(t, db, nil)
	sum6, c6 := factory.CommitRandom(t, db, [][]byte{sum4, sum5})
	require.NoError(t, ref.CommitHead(rs, "main", sum6, c6, nil))

	finder := apiutils.NewClosedSetsFinder(db, rs, 2)
	acks, err := finder.Process([][]byte{sum4}, nil, true)
	require.NoError(t, err)
	assert.Nil(t, acks)
	commits, err := finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c1, c2, c3, c4}, commits, true)
	tables, err := finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c3.Table): {},
		string(c4.Table): {},
	}, tables, true)

	finder = apiutils.NewClosedSetsFinder(db, rs, 3)
	acks, err = finder.Process([][]byte{sum6}, [][]byte{sum1}, true)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum1}, acks)
	assert.Equal(t, [][]byte{sum1}, finder.CommonCommmits())
	commits, err = finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c6, c5, c4, c3, c2}, commits, true)
	tables, err = finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c6.Table): {},
		string(c5.Table): {},
		string(c4.Table): {},
		string(c3.Table): {},
	}, tables, true)
}

func TestIncludeLeftOverWants(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, c1 := factory.CommitRandom(t, db, nil)
	sum2, c2 := factory.CommitRandom(t, db, nil)
	sum3, c3 := factory.CommitRandom(t, db, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rs, "alpha", sum1, c1, nil))
	require.NoError(t, ref.CommitHead(rs, "beta", sum3, c3, nil))

	finder := apiutils.NewClosedSetsFinder(db, rs, 0)
	acks, err := finder.Process([][]byte{sum3}, [][]byte{sum1}, false)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum1}, acks)
	assert.Equal(t, [][]byte{sum1}, finder.CommonCommmits())
	commits, err := finder.CommitsToSend()
	require.NoError(t, err)
	objhelpers.AssertCommitsEqual(t, []*objects.Commit{c3, c2}, commits, true)
	tables, err := finder.TablesToSend()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		string(c2.Table): {},
		string(c3.Table): {},
	}, tables)
}
