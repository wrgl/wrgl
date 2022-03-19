package refsql

import (
	"database/sql"
	"io"

	"github.com/wrgl/wrgl/pkg/ref"
)

type ReflogReader struct {
	db      *sql.DB
	ref     string
	ordinal int
}

func (l *ReflogReader) Read() (*ref.Reflog, error) {
	if l.ordinal == 0 {
		return nil, io.EOF
	}
	row := l.db.QueryRow(
		`SELECT oldoid, newoid, authorname, authoremail, time, action, message
		FROM reflogs WHERE ref = ? AND ordinal = ?`,
		l.ref, l.ordinal,
	)
	rl := &ref.Reflog{}
	if err := row.Scan(&rl.OldOID, &rl.NewOID, &rl.AuthorName, &rl.AuthorEmail, &rl.Time, &rl.Action, &rl.Message); err != nil {
		return nil, err
	}
	l.ordinal -= 1
	return rl, nil
}

func (l *ReflogReader) Close() error {
	return nil
}
