package testutils

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/sqlutil"
)

func CreateSQLDB(t *testing.T, createTableStatements []string) (db *sql.DB, stop func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
	require.NoError(t, err)
	require.NoError(t, sqlutil.RunInTx(db, func(tx *sql.Tx) error {
		for _, stmt := range createTableStatements {
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
