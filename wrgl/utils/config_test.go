// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/testutils"
)

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

func randomConfig() *conf.Config {
	return &conf.Config{
		User: &conf.ConfigUser{
			Name:  testutils.BrokenRandomAlphaNumericString(8),
			Email: testutils.BrokenRandomAlphaNumericString(10),
		},
	}
}

func TestOpenSystemConfig(t *testing.T) {
	cleanup := MockSystemConf(t)
	defer cleanup()

	c1 := randomConfig()
	c1.Path = systemConfigPath
	require.NoError(t, SaveConfig(c1))

	c2, err := OpenConfig(true, false, "", "")
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestOpenGlobalConfig(t *testing.T) {
	for _, b := range []bool{true, false} {
		cleanup := MockGlobalConf(t, b)
		defer cleanup()

		c1, err := OpenConfig(false, true, "", "")
		require.NoError(t, err)
		c1.User = &conf.ConfigUser{
			Name:  "John Doe",
			Email: "john@domain.com",
		}
		require.NoError(t, SaveConfig(c1))

		c2, err := OpenConfig(false, true, "", "")
		require.NoError(t, err)
		assert.Equal(t, c1, c2)
	}
}

func TestOpenLocalConfig(t *testing.T) {
	rd, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	defer os.RemoveAll(rd)

	c1, err := OpenConfig(false, false, rd, "")
	require.NoError(t, err)
	c1.User = &conf.ConfigUser{
		Name:  "John Doe",
		Email: "john@domain.com",
	}
	require.NoError(t, SaveConfig(c1))

	c2, err := OpenConfig(false, false, rd, "")
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestOpenFileConfig(t *testing.T) {
	f, err := ioutil.TempFile("", "test_wrgl_config*.yaml")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	defer os.Remove(f.Name())

	c1 := randomConfig()
	c1.Path = f.Name()
	require.NoError(t, SaveConfig(c1))

	c2, err := OpenConfig(false, false, "", f.Name())
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestAggregateConfig(t *testing.T) {
	cleanup := MockSystemConf(t)
	defer cleanup()
	cleanup = MockGlobalConf(t, true)
	defer cleanup()
	rd, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	defer os.RemoveAll(rd)

	// write system config
	c, err := OpenConfig(true, false, rd, "")
	require.NoError(t, err)
	yes := true
	no := false
	c.Receive = &conf.ConfigReceive{
		DenyNonFastForwards: &yes,
		DenyDeletes:         &yes,
	}
	require.NoError(t, SaveConfig(c))

	// write global config
	c, err = OpenConfig(false, true, rd, "")
	require.NoError(t, err)
	c.User = &conf.ConfigUser{
		Name:  "Jane Lane",
		Email: "jane@domain.com",
	}
	require.NoError(t, SaveConfig(c))

	// write local config
	c, err = OpenConfig(false, false, rd, "")
	require.NoError(t, err)
	c.Remote = map[string]*conf.ConfigRemote{
		"origin": {
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
			},
			Push: []*conf.Refspec{
				conf.MustParseRefspec("refs/heads/main:refs/heads/main"),
			},
		},
	}
	c.Receive = &conf.ConfigReceive{
		DenyDeletes: &no,
	}
	require.NoError(t, SaveConfig(c))

	// aggregate
	c, err = AggregateConfig(rd)
	require.NoError(t, err)
	assert.Equal(t, &conf.Config{
		User: &conf.ConfigUser{
			Name:  "Jane Lane",
			Email: "jane@domain.com",
		},
		Remote: map[string]*conf.ConfigRemote{
			"origin": {
				Fetch: []*conf.Refspec{
					conf.MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
				},
				Push: []*conf.Refspec{
					conf.MustParseRefspec("refs/heads/main:refs/heads/main"),
				},
			},
		},
		Receive: &conf.ConfigReceive{
			DenyNonFastForwards: &yes,
			DenyDeletes:         &no,
		},
	}, c)
	assert.Error(t, SaveConfig(c))
}
