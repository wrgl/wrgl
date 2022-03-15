// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package authtest

import (
	"net/http"
)

type AuthzStore struct {
	m map[string][]string
}

func NewAuthzStore() *AuthzStore {
	return &AuthzStore{
		m: map[string][]string{},
	}
}

func (s *AuthzStore) AddPolicy(email, scope string) error {
	s.m[email] = append(s.m[email], scope)
	return nil
}

func (s *AuthzStore) RemovePolicy(email, scope string) error {
	for i, v := range s.m[email] {
		if v == scope {
			if i < len(s.m[email])-1 {
				s.m[email] = append(s.m[email][:i], s.m[email][i+1:]...)
			} else {
				s.m[email] = s.m[email][:i]
			}
			break
		}
	}
	return nil
}

func (s *AuthzStore) Authorized(r *http.Request, email, scope string) (bool, error) {
	if sl, ok := s.m[email]; ok {
		for _, v := range sl {
			if v == scope {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *AuthzStore) Flush() error {
	return nil
}

func (s *AuthzStore) ListPolicies(email string) (scopes []string, err error) {
	if sl, ok := s.m[email]; ok {
		return sl, nil
	}
	return nil, nil
}
