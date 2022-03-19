package refsql

import (
	"database/sql"

	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/sqlutil"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	s := &Store{
		db: db,
	}
	return s
}

func (s *Store) Set(key string, sum []byte) error {
	_, err := s.db.Exec(`INSERT INTO refs (name, sum) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET sum=excluded.sum`, key, sum)
	return err
}

func (s *Store) Get(key string) ([]byte, error) {
	row := s.db.QueryRow(`SELECT sum FROM refs WHERE name = ?`, key)
	sum := make([]byte, 16)
	if err := row.Scan(&sum); err != nil {
		return nil, ref.ErrKeyNotFound
	}
	return sum, nil
}

func (s *Store) SetWithLog(key string, sum []byte, rl *ref.Reflog) error {
	return sqlutil.RunInTx(s.db, func(tx *sql.Tx) error {
		row := tx.QueryRow(`SELECT sum FROM refs WHERE name = ?`, key)
		oldSum := make([]byte, 16)
		if err := row.Scan(&oldSum); err != nil {
			oldSum = nil
		}
		if _, err := tx.Exec(
			`INSERT INTO refs (name, sum) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET sum=excluded.sum`,
			key, sum,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO reflogs (
				ref, ordinal, oldoid, newoid, authorname, authoremail, time, action, message
			) VALUES (
				?, (
					SELECT COUNT(*)+1 FROM reflogs WHERE ref = ?
				), ?, ?, ?, ?, ?, ?, ?
			)`,
			key, key, oldSum, sum, rl.AuthorName, rl.AuthorEmail, rl.Time, rl.Action, rl.Message,
		); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) Delete(key string) error {
	return sqlutil.RunInTx(s.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM reflogs WHERE ref = ?`, key); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM refs WHERE name = ?`, key)
		return err
	})
}

func (s *Store) Filter(prefix string) (m map[string][]byte, err error) {
	rows, err := s.db.Query(`SELECT name, sum FROM refs WHERE name LIKE ?`, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m = map[string][]byte{}
	for rows.Next() {
		var name string
		var sum = make([]byte, 16)
		if err = rows.Scan(&name, &sum); err != nil {
			return nil, err
		}
		m[name] = sum
	}
	return m, nil
}

func (s *Store) FilterKey(prefix string) (keys []string, err error) {
	rows, err := s.db.Query(`SELECT name FROM refs WHERE name LIKE ? ORDER BY name`, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		keys = append(keys, name)
	}
	return keys, nil
}

func (s *Store) Rename(oldKey, newKey string) (err error) {
	return sqlutil.RunInTx(s.db, func(tx *sql.Tx) error {
		row := tx.QueryRow(`SELECT sum FROM refs WHERE name = ?`, oldKey)
		sum := make([]byte, 16)
		if err := row.Scan(&sum); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO refs (name, sum) VALUES (?, ?)`, newKey, sum); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE reflogs SET ref = ? WHERE ref = ?`, newKey, oldKey); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM refs WHERE name = ?`, oldKey); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) Copy(srcKey, dstKey string) (err error) {
	return sqlutil.RunInTx(s.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO refs (name, sum) VALUES (?, (SELECT sum FROM refs WHERE name = ?))`,
			dstKey, srcKey,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO reflogs
			SELECT ? AS ref, ordinal, oldoid, newoid, authorname, authoremail, time, action, message
			FROM reflogs WHERE ref = ?`,
			dstKey, srcKey,
		); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) LogReader(key string) (ref.ReflogReader, error) {
	row := s.db.QueryRow(`SELECT COUNT(*) FROM reflogs WHERE ref = ?`, key)
	var c int
	if err := row.Scan(&c); err != nil {
		return &ReflogReader{}, nil
	}
	return &ReflogReader{db: s.db, ref: key, ordinal: c}, nil
}
