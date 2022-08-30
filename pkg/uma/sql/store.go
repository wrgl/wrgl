package umasql

import "database/sql"

var CreateTableStmts = []string{
	`CREATE TABLE resources (
		id TEXT NOT NULL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE
	)`,
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

func (s *Store) Set(name, id string) error {
	_, err := s.db.Exec(`INSERT INTO resources (name, id) VALUES (?, ?)`, name, id)
	return err
}

func (s *Store) Get(name string) (id string, err error) {
	row := s.db.QueryRow(`SELECT id FROM resources WHERE name = ?`, name)
	var ns = sql.NullString{}
	if err = row.Scan(&ns); err != nil {
		return
	}
	id = ns.String
	return
}
