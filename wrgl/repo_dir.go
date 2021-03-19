package main

import (
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

type repoDir struct {
	rootDir string
	name    string
}

func (d *repoDir) fullPath() string {
	return filepath.Join(d.rootDir, d.name+".wrgl")
}

func (d *repoDir) FilesPath() string {
	return filepath.Join(d.fullPath(), "files")
}

func (d *repoDir) KVPath() string {
	return filepath.Join(d.fullPath(), "kv")
}

func (d *repoDir) OpenKVStore(badgerLogDebug, badgerLogInfo bool) (kv.Store, error) {
	opts := badger.DefaultOptions(d.KVPath()).
		WithLoggingLevel(badger.ERROR)
	if badgerLogDebug {
		opts = opts.WithLoggingLevel(badger.DEBUG)
	} else if badgerLogInfo {
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

func (d *repoDir) Init(useBigTableStore bool) error {
	fp := d.fullPath()
	err := os.Mkdir(fp, 0755)
	if err != nil {
		return err
	}
	err = os.Mkdir(d.FilesPath(), 0755)
	if err != nil {
		return err
	}
	err = os.Mkdir(d.KVPath(), 0755)
	if err != nil {
		return err
	}
	kvStore, err := d.OpenKVStore(false, false)
	if err != nil {
		return err
	}
	defer kvStore.Close()
	r := &versioning.Repo{}
	if useBigTableStore {
		r.TableStoreType = table.Big
	}
	return r.Save(kvStore)
}

func (d *repoDir) Exist() bool {
	fp := d.fullPath()
	_, err := os.Stat(fp)
	return err == nil
}
