// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/testutils"
)

func hexFromString(t *testing.T, s string) *payload.Hex {
	h := &payload.Hex{}
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf("%q", s)), h))
	return h
}

func (s *testSuite) TestDiffHandler(t *testing.T) {
	repo, cli, m, cleanup := s.NewClient(t)
	defer cleanup()
	db := s.getDB(repo)

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

	_, err := cli.Diff(testutils.SecureRandomBytes(16), sum2)
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	_, err = cli.Diff(sum1, testutils.SecureRandomBytes(16))
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	dr, err := cli.Diff(sum1, sum2)
	require.NoError(t, err)
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

	// pass custom headers
	req := m.Capture(t, func(header http.Header) {
		header.Set("Asdf", "1234")
		dr, err = cli.Diff(sum1, sum2, apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, dr)
	})
	assert.Equal(t, "1234", req.Header.Get("Asdf"))
}
