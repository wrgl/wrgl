// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package server_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func (s *testSuite) TestProfileHandlers(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)

	sum, com := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)

	_, err := cli.GetTableProfile(testutils.SecureRandomBytes(16))
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")
	_, err = cli.GetCommitProfile(testutils.SecureRandomBytes(16))
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	tProf, err := cli.GetTableProfile(com.Table)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), tProf.RowsCount)
	assert.Len(t, tProf.Columns, 3)
	tProf2, err := cli.GetCommitProfile(sum)
	require.NoError(t, err)
	assert.Equal(t, tProf, tProf2)
}
