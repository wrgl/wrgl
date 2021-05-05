package versioning

import (
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/core/pkg/kv"
)

type RepoDir struct {
	RootDir        string
	badgerLogInfo  bool
	badgerLogDebug bool
}

func NewRepoDir(rootDir string, badgerLogInfo, badgerLogDebug bool) *RepoDir {
	return &RepoDir{
		RootDir:        rootDir,
		badgerLogInfo:  badgerLogInfo,
		badgerLogDebug: badgerLogDebug,
	}
}

func (d *RepoDir) FullPath() string {
	return filepath.Join(d.RootDir, ".wrgl")
}

func (d *RepoDir) FilesPath() string {
	return filepath.Join(d.FullPath(), "files")
}

func (d *RepoDir) KVPath() string {
	return filepath.Join(d.FullPath(), "kv")
}

func (d *RepoDir) OpenKVStore() (kv.Store, error) {
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

func (d *RepoDir) OpenFileStore() kv.FileStore {
	return kv.NewFileStore(d.FilesPath())
}

func (d *RepoDir) Init() error {
	fp := d.FullPath()
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

func (d *RepoDir) Exist() bool {
	fp := d.FullPath()
	_, err := os.Stat(fp)
	return err == nil
}
