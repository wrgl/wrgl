package flatdb

import (
	_ "embed"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
)

//go:embed casbin_model.conf
var casbinModel string

type AuthzStore struct {
	e *casbin.Enforcer
}

func NewAuthzStore(fp string) (s *AuthzStore, err error) {
	m, err := model.NewModelFromString(casbinModel)
	if err != nil {
		return
	}
	e, err := casbin.NewEnforcer(m, fp)
	if err != nil {
		return
	}
	e.EnableAutoSave(true)
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
