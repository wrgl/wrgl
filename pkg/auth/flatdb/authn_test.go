package flatdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestAuthnStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_flatdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	authnFile := filepath.Join(dir, "authn.csv")
	s, err := NewAuthnStore(authnFile)
	require.NoError(t, err)

	peoples := make([][]string, 10)
	for i := range peoples {
		peoples[i] = []string{
			fmt.Sprintf("%s@%s.com", testutils.BrokenRandomLowerAlphaString(8), testutils.BrokenRandomLowerAlphaString(8)),
			testutils.BrokenRandomAlphaNumericString(10),
		}
		require.NoError(t, s.SetPassword(peoples[i][0], peoples[i][1]))
	}

	for _, sl := range peoples {
		assert.True(t, s.CheckPassword(sl[0], sl[1]))
		assert.False(t, s.CheckPassword(sl[0], testutils.BrokenRandomAlphaNumericString(10)))
	}

	s, err = NewAuthnStore(authnFile)
	require.NoError(t, err)
	for _, sl := range peoples {
		assert.True(t, s.CheckPassword(sl[0], sl[1]))
		assert.False(t, s.CheckPassword(sl[0], testutils.BrokenRandomAlphaNumericString(10)))
		require.NoError(t, s.RemoveUser(sl[0]))
	}
	for _, sl := range peoples {
		assert.False(t, s.CheckPassword(sl[0], sl[1]))
	}

	s, err = NewAuthnStore(authnFile)
	require.NoError(t, err)
	for _, sl := range peoples {
		assert.False(t, s.CheckPassword(sl[0], sl[1]))
	}
}
