package config

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveConfig(t *testing.T) {
	f, err := ioutil.TempFile("", "test_config*.yaml")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	defer os.Remove(f.Name())
	c := &Config{
		User: &ConfigUser{
			Name:  "John Doe",
			Email: "john@doe.com",
		},
		Remote: map[string]*ConfigRemote{
			"origin": {
				Fetch: []string{
					"+refs/heads/*:refs/remotes/origin/*",
				},
				Push: []string{
					"refs/heads/main:refs/heads/main",
				},
			},
		},
		path: f.Name(),
	}
	err = c.Save()
	require.NoError(t, err)

	c2, err := readConfig(f.Name())
	require.NoError(t, err)
	assert.Equal(t, c, c2)
}
