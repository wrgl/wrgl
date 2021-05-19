package main

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
)

func TestReflogCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum1, c1 := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	sum2, c2 := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"reflog", "alpha", "--no-pager"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("%s alpha@{0}: commit: %s", hex.EncodeToString(sum2)[:7], c2.Message),
		fmt.Sprintf("%s alpha@{1}: commit: %s", hex.EncodeToString(sum1)[:7], c1.Message),
		"",
	}, "\n"))

	// test reflog exist
	cmd = newRootCmd()
	cmd.SetArgs([]string{"reflog", "exist", "alpha"})
	assertCmdOutput(t, cmd, "reflog for \"alpha\" does exist\n")
	cmd = newRootCmd()
	cmd.SetArgs([]string{"reflog", "exist", "beta"})
	assert.Equal(t, fmt.Errorf("no such ref: \"beta\""), cmd.Execute())
}
