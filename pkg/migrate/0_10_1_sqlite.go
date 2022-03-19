package migrate

import (
	"bufio"
	"container/list"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func init() {
	type Reflog struct {
		OldOID      []byte
		NewOID      []byte
		AuthorName  string
		AuthorEmail string
		Time        time.Time
		Action      string
		Message     string
	}

	const (
		rlStateOldOID int = iota
		rlStateNewOID
		rlStateName
		rlStateEmail
		rlStateTime
		rlStateAction
		rlStateMessage
	)

	refPath := func(dir, ref string) string {
		return filepath.Join(dir, "files", "refs", ref)
	}

	reflogPath := func(dir, ref string) string {
		return filepath.Join(dir, "files", "logs", ref)
	}

	eachRef := func(dir string, cb func(key string, sum []byte) error) (err error) {
		paths := list.New()
		paths.PushBack("")
		for e := paths.Front(); e != nil; e = e.Next() {
			var p = e.Value.(string)
			entries, err := os.ReadDir(refPath(dir, p))
			if err != nil {
				if _, ok := err.(*os.PathError); ok {
					continue
				}
				return err
			}
			for _, f := range entries {
				if f.IsDir() {
					paths.PushBack(path.Join(p, f.Name()))
				} else {
					key := path.Join(p, f.Name())
					b, err := os.ReadFile(refPath(dir, key))
					if err != nil {
						return err
					}
					if err = cb(key, b); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	decodeTime := func(s string) (t time.Time, err error) {
		sec, err := strconv.ParseInt(s[0:10], 10, 64)
		if err != nil {
			return
		}
		t = time.Unix(sec, 0)
		tz, err := time.Parse("-0700", s[11:16])
		if err != nil {
			return
		}
		t = t.In(tz.Location())
		return
	}

	readReflog := func(b []byte) (n int, rec *Reflog, err error) {
		state := rlStateOldOID
		off := 0
		n = len(b)
		line := string(b)
		rec = &Reflog{}
	mainLoop:
		for {
			switch state {
			case rlStateOldOID:
				if !strings.HasPrefix(line, strings.Repeat("0", 32)) {
					rec.OldOID = make([]byte, 16)
					_, err = hex.Decode(rec.OldOID, []byte(b[:32]))
					if err != nil {
						return 0, nil, err
					}
				}
				off += 33
				state = rlStateNewOID
			case rlStateNewOID:
				rec.NewOID = make([]byte, 16)
				_, err = hex.Decode(rec.NewOID, []byte(b[off:off+32]))
				if err != nil {
					return 0, nil, err
				}
				off += 33
				state = rlStateName
			case rlStateName:
				for i := off + 1; i < n; i++ {
					c := b[i]
					if c == '<' {
						rec.AuthorName = line[off : i-1]
						state = rlStateEmail
						off = i + 1
						break
					} else if c >= 48 && c <= 57 {
						// c is a numeric rune
						rec.AuthorName = line[off : i-1]
						state = rlStateTime
						off = i
						break
					}
				}
				if rec.AuthorName == "" {
					return 0, nil, fmt.Errorf("invalid reflog record: couldn't parse author name in record %q", line)
				}
			case rlStateEmail:
				for i := off + 1; i < n; i++ {
					c := b[i]
					if c == '>' {
						rec.AuthorEmail = line[off:i]
						state = rlStateTime
						off = i + 2
						break
					}
				}
				if rec.AuthorEmail == "" {
					return 0, nil, fmt.Errorf("invalid reflog record: couldn't parse author email in record %q", line)
				}
			case rlStateTime:
				rec.Time, err = decodeTime(line[off:])
				if err != nil {
					return 0, nil, err
				}
				state = rlStateAction
				off += 17
			case rlStateAction:
				for i := off + 1; i < n; i++ {
					c := b[i]
					if c == ':' {
						rec.Action = line[off:i]
						state = rlStateMessage
						off = i + 2
						break
					}
				}
				if rec.Action == "" {
					return 0, nil, fmt.Errorf("invalid reflog record: couldn't parse action in record %q", line)
				}
			case rlStateMessage:
				rec.Message = line[off:]
				break mainLoop
			}
		}
		return n, rec, nil
	}

	eachReflog := func(dir, ref string, cb func(rl *Reflog) error) error {
		f, err := os.Open(reflogPath(dir, ref))
		if err != nil {
			return nil
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			_, rl, err := readReflog(scanner.Bytes())
			if err != nil {
				return err
			}
			if err = cb(rl); err != nil {
				return err
			}
		}
		return scanner.Err()
	}

	migrations = insertMigration(migrations, migration{
		SemVer: &SemVer{0, 10, 1},
		Migrate: func(dir string) error {
			db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
			if err != nil {
				return err
			}
			defer db.Close()
			if err = runInTx(db, func(tx *sql.Tx) error {
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
				refStmt, err := tx.Prepare(`INSERT INTO refs(name, sum) VALUES(?, ?)`)
				if err != nil {
					return err
				}
				reflogStmt, err := tx.Prepare(`INSERT INTO reflogs(
					ref, ordinal, oldoid, newoid, authorname, authoremail, time, action, message
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
				if err != nil {
					return err
				}
				defer refStmt.Close()
				if err = eachRef(dir, func(key string, sum []byte) error {
					if _, err = refStmt.Exec(key, sum); err != nil {
						return err
					}
					ordinal := 1
					return eachReflog(dir, key, func(rl *Reflog) error {
						_, err = reflogStmt.Exec(key, ordinal, rl.OldOID, rl.NewOID, rl.AuthorName, rl.AuthorEmail, rl.Time, rl.Action, rl.Message)
						if err != nil {
							return err
						}
						ordinal += 1
						return nil
					})
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			if err = os.RemoveAll(reflogPath(dir, "")); err != nil {
				return err
			}
			return os.RemoveAll(refPath(dir, ""))
		},
	})
}
