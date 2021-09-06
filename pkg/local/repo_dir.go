// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package local

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/core/pkg/objects"
	objbadger "github.com/wrgl/core/pkg/objects/badger"
	"github.com/wrgl/core/pkg/ref"
	reffs "github.com/wrgl/core/pkg/ref/fs"
)

type RepoDir struct {
	FullPath       string
	badgerLogInfo  bool
	badgerLogDebug bool
}

func NewRepoDir(wrglDir string, badgerLogInfo, badgerLogDebug bool) *RepoDir {
	return &RepoDir{
		FullPath:       wrglDir,
		badgerLogInfo:  badgerLogInfo,
		badgerLogDebug: badgerLogDebug,
	}
}

func (d *RepoDir) FilesPath() string {
	return filepath.Join(d.FullPath, "files")
}

func (d *RepoDir) KVPath() string {
	return filepath.Join(d.FullPath, "kv")
}

func (d *RepoDir) openBadger() (*badger.DB, error) {
	opts := badger.DefaultOptions(d.KVPath()).
		WithLoggingLevel(badger.ERROR)
	if d.badgerLogDebug {
		opts = opts.WithLoggingLevel(badger.DEBUG)
	} else if d.badgerLogInfo {
		opts = opts.WithLoggingLevel(badger.INFO)
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
	return reffs.NewStore(d.FilesPath())
}

func (d *RepoDir) Init() error {
	err := os.Mkdir(d.FullPath, 0755)
	if err != nil {
		return err
	}
	err = os.Mkdir(d.FilesPath(), 0755)
	if err != nil {
		return err
	}
	return os.Mkdir(d.KVPath(), 0755)
}

func (d *RepoDir) Exist() bool {
	_, err := os.Stat(d.FullPath)
	return err == nil
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
