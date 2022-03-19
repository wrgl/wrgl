// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package local

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/fsnotify/fsnotify"
	"github.com/wrgl/wrgl/pkg/migrate"
	"github.com/wrgl/wrgl/pkg/objects"
	objbadger "github.com/wrgl/wrgl/pkg/objects/badger"
	"github.com/wrgl/wrgl/pkg/ref"
	refsql "github.com/wrgl/wrgl/pkg/ref/sql"
)

type RepoDir struct {
	FullPath  string
	badgerLog string
	watcher   *fsnotify.Watcher
	db        *sql.DB
}

func NewRepoDir(wrglDir string, badgerLog string) (*RepoDir, error) {
	rd := &RepoDir{
		FullPath:  wrglDir,
		badgerLog: strings.ToLower(badgerLog),
	}
	_, err := os.Stat(wrglDir)
	if err == nil {
		if err = migrate.Migrate(wrglDir); err != nil {
			return nil, err
		}
	}
	rd.db, err = sql.Open("sqlite3", filepath.Join(wrglDir, "sqlite.db"))
	if err != nil {
		return nil, err
	}
	return rd, nil
}

// Watcher returns a watcher that watch the repository directory
func (d *RepoDir) Watcher() (*fsnotify.Watcher, error) {
	if d.watcher == nil {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
		if err := watcher.Add(d.FullPath); err != nil {
			return nil, err
		}
		d.watcher = watcher
	}
	return d.watcher, nil
}

func (d *RepoDir) KVPath() string {
	return filepath.Join(d.FullPath, "kv")
}

func (d *RepoDir) openBadger() (*badger.DB, error) {
	opts := badger.DefaultOptions(d.KVPath()).
		WithLoggingLevel(badger.ERROR)
	switch d.badgerLog {
	case "debug":
		opts = opts.WithLoggingLevel(badger.DEBUG)
	case "info":
		opts = opts.WithLoggingLevel(badger.INFO)
	case "warning":
		opts = opts.WithLoggingLevel(badger.WARNING)
	}
	return badger.Open(opts)
}

func (d *RepoDir) OpenObjectsStore() (objects.Store, error) {
	badgerDB, err := d.openBadger()
	if err != nil {
		return nil, err
	}
	return objbadger.NewStore(badgerDB), nil
}

func (d *RepoDir) OpenObjectsTransaction() (*objbadger.Txn, error) {
	badgerDB, err := d.openBadger()
	if err != nil {
		return nil, err
	}
	return objbadger.NewTxn(badgerDB), nil
}

func (d *RepoDir) OpenRefStore() ref.Store {
	return refsql.NewStore(d.db)
}

func (d *RepoDir) Init() error {
	if _, err := os.Stat(d.FullPath); os.IsNotExist(err) {
		if err := os.Mkdir(d.FullPath, 0755); err != nil {
			return err
		}
	}
	if err := os.Mkdir(d.KVPath(), 0755); err != nil {
		return err
	}
	return migrate.Migrate(d.FullPath)
}

func (d *RepoDir) Exist() bool {
	_, err := os.Stat(d.KVPath())
	if err != nil {
		return false
	}
	return true
}

func FindWrglDir() (string, error) {
	d, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	home, _ := os.UserHomeDir()
	if home != "" {
		home, err = filepath.EvalSymlinks(home)
		if err != nil {
			return "", err
		}
		if !strings.HasPrefix(d, home) {
			home = ""
		}
	}
	for {
		wd := filepath.Join(d, ".wrgl")
		_, err := os.Stat(wd)
		if err == nil {
			return wd, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if home != "" {
			if d == home {
				break
			}
		} else if filepath.Dir(d) == d {
			break
		}
		d = filepath.Dir(d)
	}
	return "", nil
}

func (d *RepoDir) Close() error {
	if d.watcher != nil {
		return d.watcher.Close()
	}
	return d.db.Close()
}
