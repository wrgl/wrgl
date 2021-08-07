// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiserver "github.com/wrgl/core/pkg/api/server"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	refmock "github.com/wrgl/core/pkg/ref/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestGetRefsHandler(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := ref.CommitHead(rs, head, sum1, commit1)
	require.NoError(t, err)
	sum2, _ := refhelpers.SaveTestCommit(t, db, nil)
	tag := "my-tag"
	err = ref.SaveTag(rs, tag, sum2)
	require.NoError(t, err)
	sum3, _ := refhelpers.SaveTestCommit(t, db, nil)
	remote := "origin"
	name := "main"
	err = ref.SaveRemoteRef(
		rs, remote, name, sum3,
		testutils.BrokenRandomAlphaNumericString(8),
		testutils.BrokenRandomAlphaNumericString(10),
		"fetch",
		"from origin",
	)
	require.NoError(t, err)
	cli, cleanup := createClient(t, apiserver.NewGetRefsHandler(rs))
	defer cleanup()

	m, err := cli.GetRefs()
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
		"tags/" + tag:   sum2,
	}, m)
}
