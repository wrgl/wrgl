package sqlutil

import (
	"database/sql"
)

type DB interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func RunInTx(db *sql.DB, run func(tx *sql.Tx) error) error {
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

func QueryRows(db DB, query string, args []interface{}, scans []interface{}, cb func() error) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return err
		}
		if err = cb(); err != nil {
			return err
		}
	}
	return rows.Err()
}
