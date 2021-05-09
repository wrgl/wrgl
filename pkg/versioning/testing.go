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

// createTimeGen create a time generator that returns a timestamp that increase by 1 second
// each time it is called. This ensures that all commits have different timestamp.
func createTimeGen() func() time.Time {
	t := time.Now()
	return func() time.Time {
		t = t.Add(time.Second)
		return t
	}
}

func init() {
	tg = createTimeGen()
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
