package api_test

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func createClient(t *testing.T, handler http.Handler) (*apiclient.Client, func()) {
	t.Helper()
	ts := httptest.NewUnstartedServer(handler)
	ts.Config.ConnState = func(c net.Conn, cs http.ConnState) {
		switch cs {
		case http.StateNew:
			fmt.Printf("connState: StateNew\n")
		case http.StateActive:
			fmt.Printf("connState: StateActive\n")
		case http.StateIdle:
			fmt.Printf("connState: StateIdle\n")
		case http.StateHijacked:
			fmt.Printf("connState: StateHijacked\n")
		case http.StateClosed:
			fmt.Printf("connState: StateClosed\n")
		}
	}
	ts.Start()
	cli, err := apiclient.NewClient(ts.URL)
	require.NoError(t, err)
	return cli, ts.Close
}

func TestCommitHandler(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	parent, parentCom := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "alpha", parent, parentCom))
	cli, cleanup := createClient(t, apiserver.NewCommitHandler(db, rs))
	defer cleanup()

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
