package server_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
)

func assertRefEqual(t *testing.T, rs ref.Store, r string, sum []byte) {
	t.Helper()
	b, err := ref.GetRef(rs, r)
	if sum == nil {
		assert.Empty(t, b)
	} else {
		require.NoError(t, err)
		assert.Equal(t, sum, b)
	}
}

func (s *testSuite) TestReceivePackHandler(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	require.NoError(t, cs.Save(server_testutils.ReceivePackConfig(false, false)))
	sum1, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	sum3, _ := factory.CommitHead(t, db, rs, "delta", nil, nil)
	sum7, _ := factory.CommitHead(t, db, rs, "theta", nil, nil)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	factory.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1, sum2, sum7})
	sum4, c4 := factory.CommitRandom(t, dbc, [][]byte{sum1})
	sum5, c5 := factory.CommitRandom(t, dbc, nil)
	sum9, _ := factory.CommitRandom(t, dbc, nil)
	sum6, c6 := factory.CommitRandom(t, dbc, [][]byte{sum9})
	sum8, c8 := factory.CommitRandom(t, dbc, nil)
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum4, c4, nil))
	require.NoError(t, ref.CommitHead(rsc, "gamma", sum5, c5, nil))
	require.NoError(t, ref.CommitHead(rsc, "delta", sum6, c6, nil))
	require.NoError(t, ref.CommitHead(rsc, "theta", sum8, c8, nil))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum1), Sum: payload.BytesToHex(sum4)}, // fast-forward
		"refs/heads/beta":  {OldSum: payload.BytesToHex(sum2)},                                // delete
		"refs/heads/gamma": {Sum: payload.BytesToHex(sum5)},                                   // create
		"refs/heads/delta": {OldSum: payload.BytesToHex(sum9), Sum: payload.BytesToHex(sum6)}, // outdated ref
		"refs/heads/theta": {OldSum: payload.BytesToHex(sum7), Sum: payload.BytesToHex(sum8)}, // non-fast-forward
	}
	updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 0)
	assert.Equal(t, "remote ref updated since checkout", updates["refs/heads/delta"].ErrMsg)
	delete(updates, "refs/heads/delta")
	updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 0)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	assert.Empty(t, updates["refs/heads/beta"].ErrMsg)
	assert.Empty(t, updates["refs/heads/gamma"].ErrMsg)
	assert.Empty(t, updates["refs/heads/theta"].ErrMsg)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1, sum4, sum5})
	assertRefEqual(t, rs, "heads/alpha", sum4)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		OldOID:      sum1,
		NewOID:      sum4,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "update ref",
	})
	assertRefEqual(t, rs, "heads/beta", nil)
	assertRefEqual(t, rs, "heads/gamma", sum5)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/gamma", &ref.Reflog{
		NewOID:      sum5,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "create ref",
	})
	assertRefEqual(t, rs, "heads/delta", sum3)
	assertRefEqual(t, rs, "heads/theta", sum8)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/theta", &ref.Reflog{
		OldOID:      sum7,
		NewOID:      sum8,
		AuthorName:  "test",
		AuthorEmail: "test@domain.com",
		Action:      "receive-pack",
		Message:     "update ref",
	})

	// delete only
	updates = map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum4)}, // fast-forward
	}
	updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 0)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	_, err = ref.GetHead(rs, "alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	assert.True(t, objects.CommitExist(db, sum4))
}

func (s *testSuite) TestReceivePackHandlerNoDeletesNoFastForwards(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	require.NoError(t, cs.Save(server_testutils.ReceivePackConfig(true, true)))
	sum1, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum3, c3 := factory.CommitRandom(t, dbc, nil)
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum3, c3, nil))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {OldSum: payload.BytesToHex(sum1), Sum: payload.BytesToHex(sum3)},
		"refs/heads/beta":  {OldSum: payload.BytesToHex(sum2)},
	}
	updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 0)
	assert.Equal(t, "remote does not support non-fast-fowards", updates["refs/heads/alpha"].ErrMsg)
	assert.Equal(t, "remote does not support deleting refs", updates["refs/heads/beta"].ErrMsg)
	assertRefEqual(t, rs, "heads/alpha", sum1)
	assertRefEqual(t, rs, "heads/beta", sum2)
}

func (s *testSuite) TestReceivePackHandlerMultiplePackfiles(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	require.NoError(t, cs.Save(server_testutils.ReceivePackConfig(true, true)))
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, _ := factory.CommitRandomN(t, dbc, 5, 1000, nil)
	sum2, c2 := factory.CommitRandomN(t, dbc, 5, 1000, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum2, c2, nil))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {Sum: payload.BytesToHex(sum2)},
	}
	updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 1024)
	assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1, sum2})
	assertRefEqual(t, rs, "heads/alpha", sum2)
}

func (s *testSuite) TestReceivePackHandlerCustomHeader(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	require.NoError(t, cs.Save(server_testutils.ReceivePackConfig(true, true)))
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, c1 := factory.CommitRandomN(t, dbc, 3, 4, nil)
	require.NoError(t, ref.CommitHead(rsc, "alpha", sum1, c1, nil))

	updates := map[string]*payload.Update{
		"refs/heads/alpha": {Sum: payload.BytesToHex(sum1)},
	}
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "qwe")
		updates = server_testutils.PushObjects(t, dbc, rsc, cli, updates, remoteRefs, 1024, apiclient.WithRequestHeader(header))
		assert.Empty(t, updates["refs/heads/alpha"].ErrMsg)
		factory.AssertCommitsPersisted(t, db, [][]byte{sum1})
		assertRefEqual(t, rs, "heads/alpha", sum1)
	})
	assert.Equal(t, "qwe", req.Header.Get("Custom-Header"))
}

func (s *testSuite) TestReceivePackRejectsShallowCommits(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	// db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	remoteRefs, err := ref.ListAllRefs(rs)
	require.NoError(t, err)

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	sum1, c1 := refhelpers.SaveTestCommit(t, dbc, nil)
	sum2, c2 := factory.CommitRandomN(t, dbc, 3, 4, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "main", sum2, c2, nil))
	_, err = apiclient.NewReceivePackSession(dbc, rsc, cli, map[string]*payload.Update{"refs/heads/main": {Sum: payload.BytesToHex(sum2)}}, remoteRefs, 0, nil)
	assert.Equal(t, apiclient.NewShallowCommitError(sum1, c1.Table), err)
}
