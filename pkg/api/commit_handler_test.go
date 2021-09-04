package api_test

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
)

func (s *testSuite) TestCommitHandler(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	parent, parentCom := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "alpha", parent, parentCom))

	// missing branch
	_, err := cli.Commit("", "", "", "", nil, nil)
	assert.Equal(t, "status 400: missing branch name", err.Error())

	// invalid branch
	_, err = cli.Commit("123 kjl", "", "", "", nil, nil)
	assert.Equal(t, "status 400: invalid branch name", err.Error())

	// missing message
	_, err = cli.Commit("alpha", "", "", "", nil, nil)
	assert.Equal(t, "status 400: missing message", err.Error())

	// missing author email
	_, err = cli.Commit("alpha", "initial commit", "", "", nil, nil)
	assert.Equal(t, "status 400: missing author email", err.Error())

	// missing author name
	_, err = cli.Commit("alpha", "initial commit", "john@doe.com", "", nil, nil)
	assert.Equal(t, "status 400: missing author name", err.Error())

	// missing file
	_, err = cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", nil, nil)
	assert.Equal(t, "status 400: missing file", err.Error())

	// valid request
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll([][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	}))
	w.Flush()
	cr, err := cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), []string{"a"})
	require.NoError(t, err)
	assert.NotEmpty(t, cr.Sum)
	assert.NotEmpty(t, cr.Table)

	com, err := objects.GetCommit(db, (*cr.Sum)[:])
	require.NoError(t, err)
	assert.Equal(t, "initial commit", com.Message)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@doe.com", com.AuthorEmail)
	assert.Equal(t, [][]byte{parent}, com.Parents)
	assert.Equal(t, (*cr.Table)[:], com.Table)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, tbl.PrimaryKey())
	assert.Equal(t, []string{"a", "b", "c"}, tbl.Columns)
	assert.Equal(t, uint32(3), tbl.RowsCount)
	assert.Len(t, tbl.Blocks, 1)
	blk, err := objects.GetBlock(db, tbl.Blocks[0])
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	}, blk)

	sum, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.Equal(t, (*cr.Sum)[:], sum)

	// pass custom headers
	buf = bytes.NewBuffer(nil)
	w = csv.NewWriter(buf)
	require.NoError(t, w.Write([]string{"a", "b", "c"}))
	require.NoError(t, w.WriteAll(testutils.BuildRawCSV(3, 4)))
	w.Flush()
	req := m.Capture(t, func(header http.Header) {
		header.Set("Abcd", "qwer")
		cr, err = cli.Commit(
			"alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), []string{"a"},
			apiclient.WithHeader(header),
		)
		require.NoError(t, err)
		assert.NotEmpty(t, cr.Sum)
	})
	assert.Equal(t, "qwer", req.Header.Get("Abcd"))
}

func (s *testSuite) TestPostCommitCallback(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, true)
	defer cleanup()
	var com = &objects.Commit{}
	var r string
	var comSum = make([]byte, 16)
	s.postCommit = func(commit *objects.Commit, sum []byte, branch string) {
		*com = *commit
		copy(comSum, sum)
		r = branch
	}
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	parent, parentCom := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "alpha", parent, parentCom))

	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll([][]string{
		{"a", "b", "c"},
		{"1", "q", "w"},
		{"2", "a", "s"},
		{"3", "z", "x"},
	}))
	w.Flush()
	cr, err := cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), []string{"a"})
	require.NoError(t, err)
	assert.Equal(t, (*cr.Table)[:], com.Table)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@doe.com", com.AuthorEmail)
	assert.False(t, com.Time.IsZero())
	assert.Equal(t, "initial commit", com.Message)
	assert.Equal(t, [][]byte{parent}, com.Parents)
	assert.Equal(t, (*cr.Sum)[:], comSum)
	assert.Equal(t, "alpha", r)
}
