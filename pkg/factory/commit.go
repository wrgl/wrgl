package factory

import (
	"testing"
	"time"

	"github.com/imdario/mergo"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func BuildAuthor() *versioning.Author {
	return &versioning.Author{
		Email: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		Name:  testutils.BrokenRandomLowerAlphaString(10),
	}
}

func CommitSmall(t *testing.T, db kv.Store, branch string, rows []string, pk []uint32, args *versioning.Commit) (string, *versioning.Commit) {
	t.Helper()
	c := &versioning.Commit{
		Author:    BuildAuthor(),
		Message:   testutils.BrokenRandomAlphaNumericString(20),
		Timestamp: time.Now(),
	}
	if args != nil {
		err := mergo.Merge(c, args, mergo.WithOverride)
		require.NoError(t, err)
	}
	sum, _ := BuildSmallTable(t, db, rows, pk)
	b, err := versioning.GetBranch(db, branch)
	if err != nil {
		b = &versioning.Branch{}
	}
	c.PrevCommitHash = b.CommitHash
	c.ContentHash = sum
	c.TableStoreType = table.Small
	sum, err = c.Save(db, 0)
	require.NoError(t, err)
	b.CommitHash = sum
	require.NoError(t, b.Save(db, branch))
	return sum, c
}

func CommitBig(t *testing.T, db kv.Store, fs kv.FileStore, branch string, rows []string, pk []uint32, args *versioning.Commit) (string, *versioning.Commit) {
	t.Helper()
	c := &versioning.Commit{
		Author:    BuildAuthor(),
		Message:   testutils.BrokenRandomAlphaNumericString(20),
		Timestamp: time.Now(),
	}
	if args != nil {
		err := mergo.Merge(c, args, mergo.WithOverride)
		require.NoError(t, err)
	}
	sum, _ := BuildBigTable(t, db, fs, rows, pk)
	b, err := versioning.GetBranch(db, branch)
	if err != nil {
		b = &versioning.Branch{}
	}
	c.PrevCommitHash = b.CommitHash
	c.ContentHash = sum
	c.TableStoreType = table.Big
	sum, err = c.Save(db, 0)
	require.NoError(t, err)
	b.CommitHash = sum
	require.NoError(t, b.Save(db, branch))
	return sum, c
}
