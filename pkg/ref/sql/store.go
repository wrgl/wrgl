package refsql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/sqlutil"
)

var CreateTableStmts = []string{
	`CREATE TABLE refs (
		name TEXT NOT NULL PRIMARY KEY,
		sum  BLOB NOT NULL
	)`,
	`CREATE TABLE transactions (
		id     BLOB NOT NULL PRIMARY KEY,
		status TEXT NOT NULL,
		begin  DATETIME NOT NULL,
		end    DATETIME
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
		txid        BLOB,
		PRIMARY KEY (ref, ordinal),
		FOREIGN KEY (ref) REFERENCES refs(name),
		FOREIGN KEY (txid) REFERENCES transactions(id)
	)`,
}

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
		var txid []byte
		if rl.Txid != nil {
			txid = (*rl.Txid)[:]
		}
		if _, err := tx.Exec(
			`INSERT INTO reflogs (
				ref, ordinal, oldoid, newoid, authorname, authoremail, time, action, message, txid
			) VALUES (
				?, (
					SELECT COUNT(*)+1 FROM reflogs WHERE ref = ?
				), ?, ?, ?, ?, ?, ?, ?, ?
			)`,
			key, key, oldSum, sum, rl.AuthorName, rl.AuthorEmail, rl.Time, rl.Action, rl.Message, txid,
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
	if err = rows.Err(); err != nil {
		return nil, err
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
	if err = rows.Err(); err != nil {
		return nil, err
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
			SELECT ? AS ref, ordinal, oldoid, newoid, authorname, authoremail, time, action, message, txid
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
	if c == 0 {
		return nil, ref.ErrKeyNotFound
	}
	return &ReflogReader{db: s.db, ref: key, ordinal: c}, nil
}

func (s *Store) NewTransaction(tx *ref.Transaction) (*uuid.UUID, error) {
	if tx == nil {
		id := uuid.New()
		tx = &ref.Transaction{
			ID:     id,
			Status: ref.TSInProgress,
			Begin:  time.Now(),
		}
	}
	var err error
	if tx.End.IsZero() {
		_, err = s.db.Exec(
			`INSERT INTO transactions (id, status, begin) VALUES (?, ?, ?)`,
			tx.ID[:], tx.Status, tx.Begin,
		)

	} else {
		_, err = s.db.Exec(
			`INSERT INTO transactions (id, status, begin, end) VALUES (?, ?, ?, ?)`,
			tx.ID[:], tx.Status, tx.Begin, tx.End,
		)
	}
	if err != nil {
		return nil, err
	}
	return &tx.ID, nil
}

func (s *Store) GetTransaction(id uuid.UUID) (*ref.Transaction, error) {
	row := s.db.QueryRow(`SELECT status, begin, end FROM transactions WHERE id = ?`, id[:])
	tx := &ref.Transaction{
		ID: id,
	}
	end := sql.NullTime{}
	if err := row.Scan(&tx.Status, &tx.Begin, &end); err != nil {
		return nil, err
	}
	if end.Valid {
		tx.End = end.Time
	}
	return tx, nil
}

func (s *Store) UpdateTransaction(tx *ref.Transaction) error {
	_, err := s.db.Exec(
		`UPDATE transactions SET status = ?, begin = ?, end = ? WHERE id = ?`,
		tx.Status, tx.Begin, tx.End, tx.ID[:],
	)
	return err
}

func (s *Store) DeleteTransaction(id uuid.UUID) error {
	return sqlutil.RunInTx(s.db, func(tx *sql.Tx) error {
		row := tx.QueryRow(`SELECT status FROM transactions WHERE id = ?`, id[:])
		var status ref.TransactionStatus
		if err := row.Scan(&status); err != nil {
			return err
		}
		if status == ref.TSCommitted {
			return fmt.Errorf("cannot discard committed transaction")
		}
		_, err := tx.Exec(`DELETE FROM transactions WHERE id = ?`, id[:])
		return err
	})
}

func (s *Store) GCTransactions(txTTL time.Duration) (ids []uuid.UUID, err error) {
	cutOffTime := time.Now().Add(-txTTL)
	rows, err := s.db.Query(
		`DELETE FROM transactions WHERE status = ? AND begin <= ? RETURNING id`,
		ref.TSInProgress, cutOffTime,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *Store) GetTransactionLogs(txid uuid.UUID) (logs map[string]*ref.Reflog, err error) {
	rows, err := s.db.Query(
		`SELECT ref, oldoid, newoid, authorname, authoremail, time, action, message
		FROM reflogs WHERE txid = ? ORDER BY ref ASC`,
		txid[:],
	)
	if err != nil {
		return
	}
	defer rows.Close()
	var (
		name                    string
		oldOID, newOID          sqlutil.NullBlob
		authorName, authorEmail string
		ts                      time.Time
		action, message         string
	)
	logs = map[string]*ref.Reflog{}
	if err = sqlutil.QueryRows(s.db,
		`SELECT ref, oldoid, newoid, authorname, authoremail, time, action, message
		FROM reflogs WHERE txid = ?`,
		[]interface{}{txid[:]},
		[]interface{}{&name, &oldOID, &newOID, &authorName, &authorEmail, &ts, &action, &message},
		func() error {
			rl := &ref.Reflog{
				Txid:        &txid,
				NewOID:      make([]byte, 16),
				AuthorName:  authorName,
				AuthorEmail: authorEmail,
				Time:        ts,
				Action:      action,
				Message:     message,
			}
			copy(rl.NewOID, newOID.Blob)
			if oldOID.Valid {
				rl.OldOID = make([]byte, 16)
				copy(rl.OldOID, oldOID.Blob)
			}
			logs[name] = rl
			return nil
		},
	); err != nil {
		return nil, err
	}
	return logs, nil
}

func (s *Store) CountTransactions() (int, error) {
	row := s.db.QueryRow(`SELECT COUNT(*) FROM transactions`)
	var c int
	if err := row.Scan(&c); err != nil {
		return 0, err
	}
	return c, nil
}

func (s *Store) ListTransactions(offset, limit int) (txs []*ref.Transaction, err error) {
	var (
		id     []byte
		status string
		begin  time.Time
		end    sql.NullTime
	)
	if err = sqlutil.QueryRows(s.db,
		`SELECT id, status, begin, end FROM transactions
		ORDER BY begin DESC LIMIT ? OFFSET ?`,
		[]interface{}{limit, offset},
		[]interface{}{&id, &status, &begin, &end},
		func() error {
			tx := &ref.Transaction{
				Status: ref.TransactionStatus(status),
				Begin:  begin,
			}
			copy(tx.ID[:], id)
			if end.Valid {
				tx.End = end.Time
			}
			txs = append(txs, tx)
			return nil
		},
	); err != nil {
		return nil, err
	}
	return txs, nil
}
