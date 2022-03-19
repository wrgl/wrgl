package migrate

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	reffs "github.com/wrgl/wrgl/pkg/ref/fs"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refsql "github.com/wrgl/wrgl/pkg/ref/sql"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestMigration(t *testing.T) {
	dir := t.TempDir()
	rs := reffs.NewStore(filepath.Join(dir, "files"))

	var sums [][]byte
	var rls []*ref.Reflog
	for i := 0; i < 4; i++ {
		sum := testutils.SecureRandomBytes(16)
		rl := refhelpers.RandomReflog()
		rl.NewOID = sum
		if i > 0 {
			rl.OldOID = sums[i-1]
		} else {
			rl.OldOID = nil
		}
		sums = append(sums, sum)
		rls = append(rls, rl)
		require.NoError(t, rs.SetWithLog("heads/main", sum, rl))
	}

	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, rs.Set("heads/temp", sum))

	for _, m := range migrations {
		if m.SemVer.String() == "0.10.1" {
			require.NoError(t, m.Migrate(dir))
			break
		}
	}

	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
	require.NoError(t, err)
	defer db.Close()
	s := refsql.NewStore(db)
	v, err := s.Get("heads/temp")
	require.NoError(t, err)
	assert.Equal(t, sum, v)
	v, err = s.Get("heads/main")
	require.NoError(t, err)
	assert.Equal(t, sums[3], v)
	refhelpers.AssertReflogReaderContains(t, s, "heads/main", rls[3], rls[2], rls[1], rls[0])
}
