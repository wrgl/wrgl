package factory

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/imdario/mergo"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func BuildAuthor() *versioning.Author {
	return &versioning.Author{
		Email: testutils.BrokenRandomLowerAlphaString(6) + "@domain.com",
		Name:  testutils.BrokenRandomLowerAlphaString(10),
	}
}

func BuildCommit(t *testing.T, db kv.DB, args *versioning.Commit) (string, *versioning.Commit) {
	t.Helper()
	c := &versioning.Commit{
		ContentHash: hex.EncodeToString(testutils.SecureRandomBytes(16)),
		Author:      BuildAuthor(),
		Message:     testutils.BrokenRandomAlphaNumericString(20),
		Timestamp:   time.Now(),
	}
	if args != nil {
		err := mergo.Merge(c, args, mergo.WithOverride)
		require.NoError(t, err)
	}
	sum, err := c.Save(db, 0)
	require.NoError(t, err)
	return sum, c
}

func BuildBranch(t *testing.T, db kv.DB, name string, args *versioning.Branch) *versioning.Branch {
	t.Helper()
	var b *versioning.Branch
	if args != nil {
		b = args
	} else {
		hash, _ := BuildCommit(t, db, nil)
		b = &versioning.Branch{
			CommitHash: hash,
		}
	}
	err := b.Save(db, name)
	require.NoError(t, err)
	return b
}
