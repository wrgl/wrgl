package main

import (
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/versioning"
)

func TestResetCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	cmd := newRootCmd()

	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum, _ := factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	factory.Commit(t, db, fs, "alpha", nil, nil, nil)
	require.NoError(t, db.Close())

	setCmdArgs(cmd, rd, cf, "reset", "alpha", hex.EncodeToString(sum))
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	b, err := versioning.GetHead(db, "alpha")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
}
