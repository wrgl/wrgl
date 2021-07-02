// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package reffs

import (
	"io"
	"os"
	"path"

	"github.com/wrgl/core/pkg/ref"
)

type Store struct {
	rootDir string
}

func NewStore(rootDir string) *Store {
	return &Store{
		rootDir: rootDir,
	}
}

func (s *Store) createParentDir(p string) error {
	return os.MkdirAll(path.Dir(p), 0755)
}

func (s *Store) writeFile(p string, b []byte) error {
	err := s.createParentDir(p)
	if err != nil {
		return err
	}
	return os.WriteFile(p, b, 0666)
}

func (s *Store) openFileToAppend(p string) (*os.File, error) {
	err := s.createParentDir(p)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
}

func (s *Store) refPath(key string) string {
	return path.Join(s.rootDir, "refs", key)
}

func (s *Store) logPath(key string) string {
	return path.Join(s.rootDir, "logs", key)
}

func (s *Store) Set(key string, val []byte) error {
	return s.writeFile(s.refPath(key), val)
}

func (s *Store) SetWithLog(key string, val []byte, log *ref.Reflog) error {
	f, err := s.openFileToAppend(s.logPath(key))
	if err != nil {
		return err
	}
	_, err = log.WriteTo(f)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	return s.Set(key, val)
}

func (s *Store) Get(key string) (val []byte, err error) {
	val, err = os.ReadFile(s.refPath(key))
	if err != nil {
		return nil, ref.ErrKeyNotFound
	}
	return val, nil
}

func (s *Store) Delete(key string) error {
	err := os.Remove(s.refPath(key))
	if err != nil {
		return err
	}
	p := s.logPath(key)
	if _, err := os.Stat(p); err == nil {
		return os.Remove(p)
	}
	return nil
}

func (s *Store) Filter(prefix string) (m map[string][]byte, err error) {
	keys, err := s.FilterKey(prefix)
	if err != nil {
		return
	}
	m = map[string][]byte{}
	dir := s.refPath(prefix)
	for _, k := range keys {
		b, err := os.ReadFile(path.Join(dir, k))
		if err != nil {
			return nil, err
		}
		m[k] = b
	}
	return
}

func (s *Store) FilterKey(prefix string) (keys []string, err error) {
	files, err := os.ReadDir(s.refPath(prefix))
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, nil
		}
		return
	}
	for _, f := range files {
		keys = append(keys, f.Name())
	}
	return
}

func (s *Store) Rename(oldKey, newKey string) (err error) {
	err = os.Rename(s.refPath(oldKey), s.refPath(newKey))
	if err != nil {
		return
	}
	p := s.logPath(oldKey)
	if _, err := os.Stat(p); err == nil {
		return os.Rename(p, s.logPath(newKey))
	}
	return
}

func (s *Store) Copy(srcKey, dstKey string) (err error) {
	srcRef, err := os.Open(s.refPath(srcKey))
	if err != nil {
		return
	}
	defer srcRef.Close()
	p := s.refPath(dstKey)
	err = s.createParentDir(p)
	if err != nil {
		return
	}
	dstRef, err := os.Create(p)
	if err != nil {
		return
	}
	defer dstRef.Close()
	_, err = io.Copy(dstRef, srcRef)
	if err != nil {
		return
	}

	srcLog, err := os.Open(s.logPath(srcKey))
	if err != nil {
		return
	}
	defer srcLog.Close()
	p = s.logPath(dstKey)
	err = s.createParentDir(p)
	if err != nil {
		return
	}
	dstLog, err := os.Create(p)
	if err != nil {
		return
	}
	defer dstLog.Close()
	_, err = io.Copy(dstLog, srcLog)
	return err
}

func (s *Store) LogReader(key string) (ref.ReflogReader, error) {
	f, err := os.Open(s.logPath(key))
	if err != nil {
		return nil, ref.ErrKeyNotFound
	}
	return NewReflogReader(f)
}
