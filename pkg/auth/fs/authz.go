// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	_ "embed"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/fsnotify/fsnotify"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/local"
)

//go:embed casbin_model.conf
var casbinModel string

type AuthzStore struct {
	e       *casbin.Enforcer
	rootDir string
	watcher *fsnotify.Watcher
	mutex   sync.Mutex
	ErrChan chan error
	done    chan struct{}
}

func NewAuthzStore(rd *local.RepoDir) (s *AuthzStore, err error) {
	s = &AuthzStore{
		rootDir: rd.FullPath,
		ErrChan: make(chan error, 1),
		done:    make(chan struct{}, 1),
	}
	if err := s.read(); err != nil {
		return nil, err
	}
	s.watcher, err = rd.Watcher()
	if err != nil {
		return nil, err
	}
	go s.watch()
	return s, nil
}

func (s *AuthzStore) filepath() string {
	return filepath.Join(s.rootDir, "authz.csv")
}

func (s *AuthzStore) read() error {
	m, err := model.NewModelFromString(casbinModel)
	if err != nil {
		return err
	}
	fp := s.filepath()
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		f, err := os.OpenFile(fp, os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	e, err := casbin.NewEnforcer(m, fileadapter.NewAdapter(fp))
	if err != nil {
		return err
	}
	s.e = e
	return nil
}

func (s *AuthzStore) reload() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.e.LoadPolicy()
}

func (s *AuthzStore) watch() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				if event.Name == s.filepath() {
					if err := s.reload(); err != nil {
						s.ErrChan <- err
					}
				}
			}
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			s.ErrChan <- err
		case <-s.done:
			return
		}
	}
}

func (s *AuthzStore) AddPolicy(email, act string) error {
	_, err := s.e.AddPolicy(email, "-", act)
	return err
}

func (s *AuthzStore) RemovePolicy(email, act string) error {
	_, err := s.e.RemovePolicy(email, "-", act)
	return err
}

func (s *AuthzStore) Authorized(r *http.Request, email, scope string) (bool, error) {
	ok, err := s.e.Enforce(email, "-", scope)
	if err != nil {
		return false, err
	}
	if !ok {
		return s.e.Enforce(auth.Anyone, "-", scope)
	}
	return ok, nil
}

func (s *AuthzStore) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.e.SavePolicy()
}

func (s *AuthzStore) ListPolicies(email string) (scopes []string, err error) {
	policies := s.e.GetFilteredPolicy(0, email, "-")
	scopes = make([]string, len(policies))
	for i, sl := range policies {
		scopes[i] = sl[2]
	}
	return scopes, nil
}

func (s *AuthzStore) Close() {
	close(s.done)
	close(s.ErrChan)
}
