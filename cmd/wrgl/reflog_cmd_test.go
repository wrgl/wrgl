// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
)

func TestReflogCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum1, c1 := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum2, c2 := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"reflog", "alpha", "--no-pager"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s alpha@{0}: commit: %s", hex.EncodeToString(sum2)[:7], c2.Message),
		fmt.Sprintf("%s alpha@{1}: commit: %s", hex.EncodeToString(sum1)[:7], c1.Message),
		"",
	}, "\n"))

	// test reflog exist
	cmd = RootCmd()
	cmd.SetArgs([]string{"reflog", "exist", "alpha"})
	assertCmdOutput(t, cmd, "reflog for \"alpha\" does exist\n")
	cmd = RootCmd()
	cmd.SetArgs([]string{"reflog", "exist", "beta"})
	assert.Equal(t, fmt.Errorf("no such ref: \"beta\""), cmd.Execute())
}
