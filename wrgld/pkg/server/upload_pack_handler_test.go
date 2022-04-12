package server_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/factory"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
)

func (s *testSuite) TestUploadPack(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := factory.CommitRandomN(t, db, 5, 1000, nil)
	sum2, c2 := factory.CommitRandomN(t, db, 5, 1000, [][]byte{sum1})
	sum3, _ := factory.CommitRandomN(t, db, 5, 1000, nil)
	sum4, _ := factory.CommitRandomN(t, db, 5, 1000, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2, nil))
	require.NoError(t, ref.SaveTag(rs, "v1", sum4))

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	commits := server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2})
	assert.Equal(t, [][]byte{sum1, sum2}, commits)
	factory.AssertCommitsPersisted(t, db, commits)

	factory.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rsc, "main", sum1, c1, nil))
	_, err := apiclient.NewUploadPackSession(db, rs, cli, [][]byte{sum2})
	assert.Error(t, err, "nothing wanted")

	factory.CopyCommitsToNewStore(t, db, dbc, [][]byte{sum3})
	require.NoError(t, ref.SaveTag(rsc, "v0", sum3))
	commits = server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2, sum4},
		apiclient.WithUploadPackHavesPerRoundTrip(1),
	)
	factory.AssertCommitsPersisted(t, db, commits)
}

func (s *testSuite) TestUploadPackMultiplePackfiles(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	cs := s.s.GetConfS(repo)
	c, err := cs.Open()
	if err != nil {
		panic(err)
	}
	c.Pack = &conf.Pack{
		MaxFileSize: 1024,
	}
	sum1, _ := factory.CommitRandomN(t, db, 5, 1000, nil)
	sum2, c2 := factory.CommitRandomN(t, db, 5, 1000, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2, nil))

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	commits := server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2})
	assert.Equal(t, [][]byte{sum1, sum2}, commits)
	factory.AssertCommitsPersisted(t, db, commits)
}

func (s *testSuite) TestUploadPackCustomHeader(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := factory.CommitRandomN(t, db, 3, 4, nil)
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1, nil))

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "asd")
		commits := server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum1},
			apiclient.WithUploadPackRequestOptions(
				apiclient.WithRequestHeader(header),
			),
		)
		assert.Equal(t, [][]byte{sum1}, commits)
		factory.AssertCommitsPersisted(t, db, commits)
	})
	assert.Equal(t, "asd", req.Header.Get("Custom-Header"))
}

func (s *testSuite) TestUploadPackWithDepth(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := factory.CommitRandomN(t, db, 3, 4, nil)
	sum2, c2 := factory.CommitRandomN(t, db, 3, 4, [][]byte{sum1})
	sum3, c3 := factory.CommitRandomN(t, db, 3, 4, [][]byte{sum2})
	sum4, c4 := factory.CommitRandomN(t, db, 3, 4, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum4, c4, nil))

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()
	commits := server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum4},
		apiclient.WithUploadPackDepth(2),
	)
	testutils.AssertBytesEqual(t, [][]byte{sum4, sum3, sum2, sum1}, commits, true)
	factory.AssertCommitsShallowlyPersisted(t, dbc, commits)
	factory.AssertTablesPersisted(t, dbc, [][]byte{c4.Table, c3.Table})
	factory.AssertTablesNotPersisted(t, dbc, [][]byte{c2.Table, c1.Table})

	// get missing tables with GetObjects
	pr, err := cli.GetObjects([][]byte{c2.Table, c1.Table})
	require.NoError(t, err)
	defer pr.Close()
	or := apiutils.NewObjectReceiver(dbc, nil)
	done, err := or.Receive(pr, nil)
	require.NoError(t, err)
	assert.True(t, done)
	factory.AssertTablesPersisted(t, dbc, [][]byte{c2.Table, c1.Table})
}

func (s *testSuite) TestUploadPackSkipTables(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "alpha", sum1, c1, nil))
	sum2, c2 := factory.CommitRandomWithTable(t, db, c1.Table, nil)
	require.NoError(t, ref.CommitHead(rs, "beta", sum2, c2, nil))

	dbc := objmock.NewStore()
	rsc, cleanup := refmock.NewStore(t)
	defer cleanup()

	commits := server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum1})
	testutils.AssertBytesEqual(t, [][]byte{sum1}, commits, true)
	// assert c2.Table is not pulled
	objs := [][]byte{}
	commits = server_testutils.FetchObjects(t, dbc, rsc, cli, [][]byte{sum2},
		apiclient.WithUploadPackReceiverOptions(
			apiutils.WithReceiverSaveObjectHook(func(objType int, sum []byte) {
				objs = append(objs, sum)
				assert.Equal(t, packfile.ObjectCommit, objType)
				assert.Equal(t, sum2, sum)
			}),
		),
	)
	assert.Len(t, objs, 1)
	testutils.AssertBytesEqual(t, [][]byte{sum2}, commits, true)
	factory.AssertCommitsPersisted(t, dbc, [][]byte{sum2})
}
