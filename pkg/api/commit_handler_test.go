package api_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func TestCommitHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	rs := refmock.NewStore()
	parent, parentCom := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "alpha", parent, parentCom))
	apitest.RegisterHandler(http.MethodPost, "/commit/", api.NewCommitHandler(db, rs))

	// missing branch
	resp := apitest.PostMultipartForm(t, "/commit/", nil, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "missing branch name\n", string(b))

	// invalid branch
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch": {"123 kjl"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "invalid branch name\n", string(b))

	// missing message
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch": {"alpha"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "missing message\n", string(b))

	// missing author email
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch":  {"alpha"},
		"message": {"initial commit"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "missing author email\n", string(b))

	// missing author email
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch":      {"alpha"},
		"message":     {"initial commit"},
		"authorEmail": {"john@doe.com"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "missing author name\n", string(b))

	// missing file
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch":      {"alpha"},
		"message":     {"initial commit"},
		"authorEmail": {"john@doe.com"},
		"authorName":  {"John Doe"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "missing file\n", string(b))

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
	resp = apitest.PostMultipartForm(t, "/commit/", map[string][]string{
		"branch":      {"alpha"},
		"message":     {"initial commit"},
		"authorName":  {"John Doe"},
		"authorEmail": {"john@doe.com"},
		"primaryKey":  {"a"},
	}, map[string]io.Reader{"file": bytes.NewReader(buf.Bytes())})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, api.CTJSON, resp.Header.Get("Content-Type"))
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	cr := &payload.CommitResponse{}
	require.NoError(t, json.Unmarshal(b, cr))
	assert.NotEmpty(t, cr.Sum)

	com, err := objects.GetCommit(db, (*cr.Sum)[:])
	require.NoError(t, err)
	assert.Equal(t, "initial commit", com.Message)
	assert.Equal(t, "John Doe", com.AuthorName)
	assert.Equal(t, "john@doe.com", com.AuthorEmail)
	assert.Equal(t, [][]byte{parent}, com.Parents)
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
}
