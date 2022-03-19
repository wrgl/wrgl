package migrate

import "database/sql"

func runInTx(db *sql.DB, run func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err = run(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
