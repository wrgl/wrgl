// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package authtest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/wrgl/wrgl/pkg/auth"
)

type AuthnStore struct {
	pass map[string]string
	name map[string]string
}

func NewAuthnStore() *AuthnStore {
	return &AuthnStore{
		pass: map[string]string{},
		name: map[string]string{},
	}
}

func (s *AuthnStore) SetName(email, name string) error {
	s.name[email] = name
	return nil
}

func (s *AuthnStore) SetPassword(email, password string) error {
	s.pass[email] = password
	return nil
}

func (s *AuthnStore) Authenticate(email, password string) (token string, err error) {
	if v, ok := s.pass[email]; ok && v == password {
		b, err := json.Marshal(&auth.Claims{
			Email: email,
			Name:  s.name[email],
		})
		if err != nil {
			return "", err
		}
		return base64.RawStdEncoding.EncodeToString(b), nil
	}
	return "", fmt.Errorf("email/password invalid")
}

func (s *AuthnStore) CheckToken(r *http.Request, token string) (*http.Request, *auth.Claims, error) {
	c := &auth.Claims{}
	b, err := base64.RawStdEncoding.DecodeString(token)
	if err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal([]byte(b), c); err != nil {
		return nil, nil, err
	}
	if _, ok := s.pass[c.Email]; ok {
		return r, c, nil
	}
	return nil, nil, fmt.Errorf("invalid token")
}

func (s *AuthnStore) RemoveUser(email string) error {
	delete(s.pass, email)
	return nil
}

func (s *AuthnStore) Flush() error {
	return nil
}

func (s *AuthnStore) ListUsers() (users [][]string, err error) {
	users = make([][]string, len(s.pass))
	for email, name := range s.name {
		users = append(users, []string{email, name})
	}
	sort.Slice(users, func(i, j int) bool {
		return users[i][0] < users[j][0]
	})
	return users, nil
}

func (s *AuthnStore) Exist(email string) bool {
	_, ok := s.pass[email]
	return ok
}
