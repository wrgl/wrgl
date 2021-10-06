// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conffs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func randomConfig() *conf.Config {
	return &conf.Config{
		User: &conf.User{
			Name:  testutils.BrokenRandomAlphaNumericString(8),
			Email: testutils.BrokenRandomAlphaNumericString(10),
		},
	}
}

func TestOpenSystemConfig(t *testing.T) {
	cleanup := confhelpers.MockSystemConf(t)
	defer cleanup()

	s := NewStore("", SystemSource, "")
	c1 := randomConfig()
	require.NoError(t, s.Save(c1))

	c2, err := s.Open()
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestOpenGlobalConfig(t *testing.T) {
	for _, b := range []bool{true, false} {
		cleanup := confhelpers.MockGlobalConf(t, b)
		defer cleanup()

		s := NewStore("", GlobalSource, "")
		c1, err := s.Open()
		require.NoError(t, err)
		c1.User = &conf.User{
			Name:  "John Doe",
			Email: "john@domain.com",
		}
		require.NoError(t, s.Save(c1))

		c2, err := s.Open()
		require.NoError(t, err)
		assert.Equal(t, c1, c2)
	}
}

func TestOpenLocalConfig(t *testing.T) {
	rd, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	defer os.RemoveAll(rd)

	s := NewStore(rd, LocalSource, "")
	c1, err := s.Open()
	require.NoError(t, err)
	c1.User = &conf.User{
		Name:  "John Doe",
		Email: "john@domain.com",
	}
	require.NoError(t, s.Save(c1))

	c2, err := s.Open()
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestOpenFileConfig(t *testing.T) {
	f, err := ioutil.TempFile("", "test_wrgl_config*.yaml")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	defer os.Remove(f.Name())

	s := NewStore("", FileSource, f.Name())
	c1 := randomConfig()
	require.NoError(t, s.Save(c1))

	c2, err := s.Open()
	require.NoError(t, err)
	assert.Equal(t, c1, c2)
}

func TestAggregateConfig(t *testing.T) {
	cleanup := confhelpers.MockSystemConf(t)
	defer cleanup()
	cleanup = confhelpers.MockGlobalConf(t, true)
	defer cleanup()
	rd, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	defer os.RemoveAll(rd)

	// write system config
	s := NewStore(rd, SystemSource, "")
	c, err := s.Open()
	require.NoError(t, err)
	yes := true
	no := false
	c.Receive = &conf.Receive{
		DenyNonFastForwards: &yes,
		DenyDeletes:         &yes,
	}
	require.NoError(t, s.Save(c))

	// write global config
	s = NewStore(rd, GlobalSource, "")
	c, err = s.Open()
	require.NoError(t, err)
	c.User = &conf.User{
		Name:  "Jane Lane",
		Email: "jane@domain.com",
	}
	require.NoError(t, s.Save(c))

	// write local config
	s = NewStore(rd, LocalSource, "")
	require.NoError(t, err)
	c.Remote = map[string]*conf.Remote{
		"origin": {
			Fetch: []*conf.Refspec{
				conf.MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
			},
			Push: []*conf.Refspec{
				conf.MustParseRefspec("refs/heads/main:refs/heads/main"),
			},
		},
	}
	c.Receive = &conf.Receive{
		DenyDeletes: &no,
	}
	require.NoError(t, s.Save(c))

	// aggregate
	s = NewStore(rd, AggregateSource, "")
	c, err = s.Open()
	require.NoError(t, err)
	assert.Equal(t, &conf.Config{
		User: &conf.User{
			Name:  "Jane Lane",
			Email: "jane@domain.com",
		},
		Remote: map[string]*conf.Remote{
			"origin": {
				Fetch: []*conf.Refspec{
					conf.MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
				},
				Push: []*conf.Refspec{
					conf.MustParseRefspec("refs/heads/main:refs/heads/main"),
				},
			},
		},
		Receive: &conf.Receive{
			DenyNonFastForwards: &yes,
			DenyDeletes:         &no,
		},
	}, c)
	assert.Error(t, s.Save(c))
}
