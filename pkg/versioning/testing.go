// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package versioning

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

var tg func() time.Time

func init() {
	tg = testutils.CreateTimeGen()
}

func SaveTestCommit(t *testing.T, db kv.DB, parents [][]byte) (sum []byte, commit *objects.Commit) {
	t.Helper()
	commit = &objects.Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        tg(),
		Message:     testutils.BrokenRandomAlphaNumericString(40),
		Parents:     parents,
	}
	var err error
	sum, err = SaveCommit(db, 0, commit)
	require.NoError(t, err)
	return sum, commit
}

func AssertLatestReflogEqual(t *testing.T, fs kv.FileStore, name string, rl *objects.Reflog) {
	t.Helper()
	r, err := fs.Reader([]byte("logs/refs/" + name))
	require.NoError(t, err)
	defer r.Close()
	rr, err := objects.NewReflogReader(r)
	require.NoError(t, err)
	obj, err := rr.Read()
	require.NoError(t, err)
	assert.Equal(t, rl.OldOID, obj.OldOID)
	assert.Equal(t, rl.NewOID, obj.NewOID)
	assert.Equal(t, rl.AuthorName, obj.AuthorName)
	assert.Equal(t, rl.AuthorEmail, obj.AuthorEmail)
	assert.Equal(t, rl.Action, obj.Action)
	assert.Equal(t, rl.Message, obj.Message)
}

func MockEnv(t *testing.T, key, val string) func() {
	t.Helper()
	orig := os.Getenv(key)
	require.NoError(t, os.Setenv(key, val))
	return func() {
		require.NoError(t, os.Setenv(key, orig))
	}
}

func MockGlobalConf(t *testing.T, setXDGConfigHome bool) func() {
	t.Helper()
	name, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	var cleanup1, cleanup2 func()
	if setXDGConfigHome {
		cleanup1 = MockEnv(t, "XDG_CONFIG_HOME", name)
	} else {
		cleanup1 = MockEnv(t, "XDG_CONFIG_HOME", "")
		cleanup2 = MockEnv(t, "HOME", name)
	}
	return func() {
		require.NoError(t, os.RemoveAll(name))
		cleanup1()
		if cleanup2 != nil {
			cleanup2()
		}
	}
}

func MockSystemConf(t *testing.T) func() {
	t.Helper()
	orig := systemConfigPath
	dir, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	systemConfigPath = filepath.Join(dir, "wrgl/config.yaml")
	return func() {
		require.NoError(t, os.RemoveAll(dir))
		systemConfigPath = orig
	}
}
