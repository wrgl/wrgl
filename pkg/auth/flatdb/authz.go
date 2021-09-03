// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package flatdb

import (
	_ "embed"
	"os"
	"path/filepath"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
)

//go:embed casbin_model.conf
var casbinModel string

type AuthzStore struct {
	e *casbin.Enforcer
}

func NewAuthzStore(rootDir string) (s *AuthzStore, err error) {
	m, err := model.NewModelFromString(casbinModel)
	if err != nil {
		return
	}
	fp := filepath.Join(rootDir, "authz.csv")
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		f, err := os.OpenFile(fp, os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	e, err := casbin.NewEnforcer(m, fileadapter.NewAdapter(fp))
	if err != nil {
		return
	}
	s = &AuthzStore{
		e: e,
	}
	return s, nil
}

func (s *AuthzStore) AddPolicy(email, act string) error {
	_, err := s.e.AddPolicy(email, "-", act)
	return err
}

func (s *AuthzStore) RemovePolicy(email, act string) error {
	_, err := s.e.RemovePolicy(email, "-", act)
	return err
}

func (s *AuthzStore) Authorized(email, act string) (bool, error) {
	return s.e.Enforce(email, "-", act)
}

func (s *AuthzStore) Flush() error {
	return s.e.SavePolicy()
}
