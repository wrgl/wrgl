package versioning

import (
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
