package refsql

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	"github.com/wrgl/wrgl/pkg/sqlutil"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func testSqliteDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
	require.NoError(t, err)
	require.NoError(t, sqlutil.RunInTx(db, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			`CREATE TABLE refs (
				name TEXT NOT NULL PRIMARY KEY,
				sum  BLOB NOT NULL
			)`,
			`CREATE TABLE reflogs (
				ref         TEXT NOT NULL,
				ordinal     INTEGER NOT NULL,
				oldoid      BLOB,
				newoid      BLOB NOT NULL,
				authorname  TEXT NOT NULL DEFAULT '',
				authoremail TEXT NOT NULL DEFAULT '',
				time        DATETIME NOT NULL,
				action      TEXT NOT NULL DEFAULT '',
				message     TEXT NOT NULL DEFAULT '',
				PRIMARY KEY (ref, ordinal),
				FOREIGN KEY (ref) REFERENCES refs(name)
			)`,
		} {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		return nil
	}))
	return db, func() {
		require.NoError(t, db.Close())
	}
}

func TestStore(t *testing.T) {
	db, cleanup := testSqliteDB(t)
	defer cleanup()
	s := NewStore(db)

	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, s.Set("heads/alpha", sum))
	v, err := s.Get("heads/alpha")
	require.NoError(t, err)
	assert.Equal(t, sum, v)

	rl1 := refhelpers.RandomReflog()
	rl1.OldOID = nil
	require.NoError(t, s.SetWithLog("heads/beta", rl1.NewOID, rl1))
	rl2 := refhelpers.RandomReflog()
	rl2.OldOID = rl1.NewOID
	require.NoError(t, s.SetWithLog("heads/beta", rl2.NewOID, rl2))
	v, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)

	refhelpers.AssertReflogReaderContains(t, s, "heads/beta", rl2, rl1)

	require.NoError(t, s.Copy("heads/beta", "heads/beta-1"))
	v, err = s.Get("heads/beta-1")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	v, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta", rl2, rl1)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta-1", rl2, rl1)

	require.NoError(t, s.Rename("heads/beta", "heads/beta-2"))
	v, err = s.Get("heads/beta-2")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	_, err = s.Get("heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta-2", rl2, rl1)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta")

	keys, err := s.FilterKey("heads/")
	require.NoError(t, err)
	assert.Equal(t, []string{
		"heads/alpha", "heads/beta-1", "heads/beta-2",
	}, keys)
	keys, err = s.FilterKey("heads/be")
	require.NoError(t, err)
	assert.Equal(t, []string{"heads/beta-1", "heads/beta-2"}, keys)

	m, err := s.Filter("heads/")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/alpha":  sum,
		"heads/beta-1": rl2.NewOID,
		"heads/beta-2": rl2.NewOID,
	}, m)
	m, err = s.Filter("heads/b")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/beta-1": rl2.NewOID,
		"heads/beta-2": rl2.NewOID,
	}, m)

	require.NoError(t, s.Delete("heads/alpha"))
	_, err = s.Get("heads/alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)

	require.NoError(t, s.Delete("heads/beta-1"))
	_, err = s.Get("heads/beta-1")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta-1")
}
