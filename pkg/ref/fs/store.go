// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package reffs

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/ref"
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
	_, err = f.Write([]byte{'\n'})
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
	for _, k := range keys {
		b, err := os.ReadFile(s.refPath(k))
		if err != nil {
			return nil, err
		}
		m[k] = b
	}
	return
}

func (s *Store) FilterKey(prefix string) (keys []string, err error) {
	paths := list.New()
	paths.PushBack(prefix)
	for e := paths.Front(); e != nil; e = e.Next() {
		p := e.Value.(string)
		entries, err := os.ReadDir(s.refPath(p))
		if err != nil {
			if _, ok := err.(*os.PathError); ok {
				continue
			}
			return nil, err
		}
		for _, f := range entries {
			if f.IsDir() {
				paths.PushBack(path.Join(p, f.Name()))
			} else {
				keys = append(keys, path.Join(p, f.Name()))
			}
		}
	}
	return
}

func (s *Store) Rename(oldKey, newKey string) (err error) {
	p := s.refPath(newKey)
	err = s.createParentDir(p)
	if err != nil {
		return
	}
	err = os.Rename(s.refPath(oldKey), p)
	if err != nil {
		return
	}
	p = s.logPath(oldKey)
	if _, err := os.Stat(p); err == nil {
		q := s.logPath(newKey)
		err = s.createParentDir(q)
		if err != nil {
			return err
		}
		return os.Rename(p, q)
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

func (s *Store) NewTransaction(tx *ref.Transaction) (*uuid.UUID, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Store) GetTransaction(id uuid.UUID) (*ref.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Store) UpdateTransaction(tx *ref.Transaction) error {
	return fmt.Errorf("not implemented")
}

func (s *Store) DeleteTransaction(id uuid.UUID) error {
	return fmt.Errorf("not implemented")
}

func (s *Store) GCTransactions(txTTL time.Duration) (ids []uuid.UUID, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Store) GetTransactionLogs(txid uuid.UUID) (logs map[string]*ref.Reflog, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Store) ListTransactions(offset, limit int) (txs []*ref.Transaction, err error) {
	return nil, fmt.Errorf("not implemented")
}
