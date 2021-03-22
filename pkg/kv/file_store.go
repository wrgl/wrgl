package kv

import (
	"io"
	"os"
	"path/filepath"
)

type File interface {
	io.ReadSeekCloser
	io.ReaderAt
}

type FileStore interface {
	Delete([]byte) error
	Exist([]byte) bool
	Writer(k []byte) (io.WriteCloser, error)
	Reader([]byte) (File, error)
	Clear([]byte) error
	Size(k []byte) (uint64, error)
}

type fileStore struct {
	dataDir string
}

func NewFileStore(dir string) FileStore {
	return &fileStore{
		dataDir: dir,
	}
}

func (s *fileStore) path(k []byte) string {
	return filepath.Join(s.dataDir, string(k))
}

func (s *fileStore) Writer(k []byte) (io.WriteCloser, error) {
	p := s.path(k)
	err := os.MkdirAll(filepath.Dir(p), 0755)
	if err != nil {
		return nil, err
	}
	return os.Create(s.path(k))
}

func (s *fileStore) Reader(k []byte) (File, error) {
	r, err := os.Open(s.path(k))
	if err != nil {
		return nil, KeyNotFoundError
	}
	return r, nil
}

func (s *fileStore) Exist(k []byte) bool {
	_, err := os.Stat(s.path(k))
	return err == nil
}

func (s *fileStore) Delete(k []byte) error {
	return os.Remove(s.path(k))
}

func (s *fileStore) Clear(prefix []byte) error {
	return os.RemoveAll(s.path(prefix))
}

func (s *fileStore) Size(k []byte) (uint64, error) {
	fi, err := os.Stat(s.path(k))
	if err != nil {
		return 0, KeyNotFoundError
	}
	return uint64(fi.Size()), nil
}
