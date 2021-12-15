// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
)

func TestResetCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := rootCmd()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd.SetArgs([]string{"reset", "alpha", hex.EncodeToString(sum)})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	b, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/alpha", &ref.Reflog{
		OldOID:      sum2,
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "reset",
		Message:     "to commit " + hex.EncodeToString(sum),
	})
}
