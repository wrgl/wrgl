package main

import (
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/core/pkg/kv"
)

type repoDir struct {
	rootDir        string
	badgerLogInfo  bool
	badgerLogDebug bool
}

func (d *repoDir) fullPath() string {
	return filepath.Join(d.rootDir, ".wrgl")
}

func (d *repoDir) FilesPath() string {
	return filepath.Join(d.fullPath(), "files")
}

func (d *repoDir) KVPath() string {
	return filepath.Join(d.fullPath(), "kv")
}

func (d *repoDir) OpenKVStore() (kv.Store, error) {
	opts := badger.DefaultOptions(d.KVPath()).
		WithLoggingLevel(badger.ERROR)
	if d.badgerLogDebug {
		opts = opts.WithLoggingLevel(badger.DEBUG)
	} else if d.badgerLogInfo {
		opts = opts.WithLoggingLevel(badger.INFO)
	}
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return kv.NewBadgerStore(badgerDB), nil
}

func (d *repoDir) OpenFileStore() kv.FileStore {
	return kv.NewFileStore(d.FilesPath())
}

func (d *repoDir) Init() error {
	fp := d.fullPath()
	err := os.Mkdir(fp, 0755)
	if err != nil {
		return err
	}
	err = os.Mkdir(d.FilesPath(), 0755)
	if err != nil {
		return err
	}
	return os.Mkdir(d.KVPath(), 0755)
}

func (d *repoDir) Exist() bool {
	fp := d.fullPath()
	_, err := os.Stat(fp)
	return err == nil
}
