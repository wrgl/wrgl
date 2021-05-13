package versioning

import (
	"testing"
	"time"

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
