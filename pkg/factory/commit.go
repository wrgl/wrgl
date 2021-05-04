package factory

import (
	"testing"
	"time"

	"github.com/imdario/mergo"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func Commit(t *testing.T, db kv.Store, fs kv.FileStore, branch string, rows []string, pk []uint32, args *objects.Commit) ([]byte, *objects.Commit) {
	t.Helper()
	c := &objects.Commit{
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		Message:     testutils.BrokenRandomAlphaNumericString(20),
		Time:        time.Now(),
	}
	if args != nil {
		err := mergo.Merge(c, args, mergo.WithOverride)
		require.NoError(t, err)
	}
	sum, _ := BuildTable(t, db, fs, rows, pk)
	commitSum, err := versioning.GetHead(db, branch)
	if err == nil {
		c.Parents = append(c.Parents, commitSum)
	}
	c.Table = sum
	// c.TableType = objects.TableType_TS_SMALL
	sum, err = versioning.SaveCommit(db, 0, c)
	require.NoError(t, err)
	require.NoError(t, versioning.SaveHead(db, branch, sum))
	return sum, c
}
