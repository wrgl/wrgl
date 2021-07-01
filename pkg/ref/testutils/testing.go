// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package reftestutils

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvfs "github.com/wrgl/core/pkg/kv/fs"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

var tg func() time.Time

func init() {
	tg = testutils.CreateTimeGen()
}

func SaveTestCommit(t *testing.T, db kvcommon.DB, parents [][]byte) (sum []byte, commit *objects.Commit) {
	t.Helper()
	commit = &objects.Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        tg(),
		Message:     testutils.BrokenRandomAlphaNumericString(40),
		Parents:     parents,
	}
	buf := bytes.NewBuffer(nil)
	_, err := commit.WriteTo(buf)
	require.NoError(t, err)
	arr := meow.Checksum(0, buf.Bytes())
	sum = arr[:]
	require.NoError(t, kv.SaveCommit(db, sum, buf.Bytes()))
	return sum, commit
}

func AssertLatestReflogEqual(t *testing.T, fs kvfs.FileStore, name string, rl *objects.Reflog) {
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
