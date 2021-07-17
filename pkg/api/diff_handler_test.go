// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/json"
	"fmt"
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
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func hexFromString(t *testing.T, s string) *payload.Hex {
	h := &payload.Hex{}
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf("%q", s)), h))
	return h
}

func TestDiffHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	apitest.RegisterHandler(http.MethodGet, `=~^/diff/[0-9a-f]+/[0-9a-f]+/\z`, api.NewDiffHandler(db))

	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	sum2, _ := factory.Commit(t, db, []string{
		"a,b,d",
		"1,q,e",
		"2,a,d",
		"3,z,c",
	}, []uint32{0}, nil)

	resp := apitest.Get(t, fmt.Sprintf("/diff/%x/%x/", testutils.SecureRandomBytes(16), sum2))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = apitest.Get(t, fmt.Sprintf("/diff/%x/%x/", sum1, testutils.SecureRandomBytes(16)))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = apitest.Get(t, fmt.Sprintf("/diff/%x/%x/", sum1, sum2))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	dr := &payload.DiffResponse{}
	require.NoError(t, json.Unmarshal(b, dr))
	assert.Equal(t, &payload.DiffResponse{
		ColDiff: &payload.ColDiff{
			Columns:    []string{"a", "b", "c"},
			OldColumns: []string{"a", "b", "d"},
			PK:         []uint32{0},
			OldPK:      []uint32{0},
		},
		RowDiff: []*payload.RowDiff{
			{
				PK:     hexFromString(t, "fd1c9513cc47feaf59fa9b76008f2521"),
				Sum:    hexFromString(t, "60f1c744d65482e468bfac458a7131fe"),
				OldSum: hexFromString(t, "472dc02a63f3a555b9b39cf6c953a3ea"),
			},
			{
				PK:        hexFromString(t, "00259da5fe4e202b974d64009944ccfe"),
				Sum:       hexFromString(t, "e4f37424a61671456b0be328e4f3719c"),
				OldSum:    hexFromString(t, "00200e2c0e125fb15980d68112d5fa52"),
				Offset:    1,
				OldOffset: 1,
			},
			{
				PK:        hexFromString(t, "e3c37d3bfd03aef8fac2794539e39160"),
				Sum:       hexFromString(t, "1c51f6044190122c554cc6794585e654"),
				OldSum:    hexFromString(t, "3ef97e9414bbb071eb8d1175e2cf3ef4"),
				Offset:    2,
				OldOffset: 2,
			},
		},
	}, dr)
}
