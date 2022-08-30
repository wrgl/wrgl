package migrate

import (
	"database/sql"
	"path/filepath"
)

func init() {
	migrations = insertMigration(migrations, migration{
		SemVer: &SemVer{0, 13, 0},
		Migrate: func(dir string) error {
			db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
			if err != nil {
				return err
			}
			defer db.Close()
			if _, err = db.Exec(
				`CREATE TABLE resources (
					id TEXT NOT NULL PRIMARY KEY,
					name TEXT NOT NULL UNIQUE
				)`,
			); err != nil {
				return err
			}
			return nil
		},
	})
}
