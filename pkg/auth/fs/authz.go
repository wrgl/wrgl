// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	_ "embed"
	"net/http"
	"os"
	"path/filepath"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/fsnotify/fsnotify"
	"github.com/wrgl/wrgl/pkg/local"
)

//go:embed casbin_model.conf
var casbinModel string

type AuthzStore struct {
	e       *casbin.Enforcer
	rootDir string
	watcher *fsnotify.Watcher
}

func NewAuthzStore(rd *local.RepoDir) (s *AuthzStore, err error) {
	s = &AuthzStore{
		rootDir: rd.FullPath,
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

func (s *AuthzStore) watch() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				if event.Name == s.filepath() {
					if err := s.e.LoadPolicy(); err != nil {
						panic(err)
					}
				}
			}
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			panic(err)
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
	return s.e.Enforce(email, "-", scope)
}

func (s *AuthzStore) Flush() error {
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
