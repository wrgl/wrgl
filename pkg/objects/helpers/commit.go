// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objhelpers

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func RandomCommit() *objects.Commit {
	return &objects.Commit{
		Table:       testutils.SecureRandomBytes(16),
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        time.Now(),
		Message:     "new commit",
		Parents: [][]byte{
			testutils.SecureRandomBytes(16),
		},
	}
}

func AssertCommitEqual(t *testing.T, a, b *objects.Commit) {
	t.Helper()
	require.Equal(t, a.Table, b.Table, "table not equal")
	require.Equal(t, a.AuthorName, b.AuthorName, "author name not equal")
	require.Equal(t, a.AuthorEmail, b.AuthorEmail, "author email not equal")
	require.Equal(t, a.Message, b.Message, "message not equal")
	require.Equal(t, a.Parents, b.Parents, "parents not equal")
	require.Equal(t, a.Time.Unix(), b.Time.Unix(), "time not equal")
	require.Equal(t, a.Time.Format("-0700"), b.Time.Format("-0700"))
}

func AssertCommitsEqual(t *testing.T, sla, slb []*objects.Commit, ignoreOrder bool) {
	t.Helper()
	require.Equal(t, len(sla), len(slb), "number of commits does not match")
	if ignoreOrder {
		sortedCopy := func(obj []*objects.Commit) []*objects.Commit {
			sl := make([]*objects.Commit, len(obj))
			copy(sl, obj)
			sort.Slice(sl, func(i, j int) bool {
				a, b := sl[i], sl[j]
				if string(a.Table) != string(b.Table) {
					return string(a.Table) < string(b.Table)
				}
				if a.AuthorName != b.AuthorName {
					return a.AuthorName < b.AuthorName
				}
				if a.AuthorEmail != b.AuthorEmail {
					return a.AuthorEmail < b.AuthorEmail
				}
				if a.Message != b.Message {
					return a.Message < b.Message
				}
				if !a.Time.Equal(b.Time) {
					return a.Time.Before(b.Time)
				}
				if len(a.Parents) != len(b.Parents) {
					return len(a.Parents) < len(b.Parents)
				}
				for k, p := range a.Parents {
					if string(p) != string(b.Parents[k]) {
						return string(p) < string(b.Parents[k])
					}
				}
				return false
			})
			return sl
		}
		sla = sortedCopy(sla)
		slb = sortedCopy(slb)
	}
	for i, a := range sla {
		AssertCommitEqual(t, a, slb[i])
	}
}
