// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/auth/random"
	"golang.org/x/crypto/bcrypt"
)

const DefaultTokenDuration time.Duration = time.Hour * 24 * 90 // 90 days

type AuthnStore struct {
	sl            [][]string
	rootDir       string
	secret        []byte
	tokenDuration time.Duration
}

func NewAuthnStore(rootDir string, tokenDuration time.Duration) (s *AuthnStore, err error) {
	if tokenDuration == 0 {
		tokenDuration = DefaultTokenDuration
	}
	s = &AuthnStore{
		rootDir:       rootDir,
		tokenDuration: tokenDuration,
	}
	f, err := os.Open(filepath.Join(rootDir, "authn.csv"))
	if err == nil {
		defer f.Close()
		r := csv.NewReader(f)
		s.sl, err = r.ReadAll()
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *AuthnStore) Len() int {
	return len(s.sl)
}

func (s *AuthnStore) getSecret() ([]byte, error) {
	if s.secret != nil {
		return s.secret, nil
	}
	fp := filepath.Join(s.rootDir, "auth_secret.txt")
	f, err := os.Open(fp)
	if err != nil {
		if os.IsNotExist(err) {
			f, err = os.OpenFile(fp, os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			sec := []byte(random.RandomAlphaNumericString(20))
			_, err = f.Write(sec)
			if err != nil {
				return nil, err
			}
			s.secret = sec
			return sec, nil
		}
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	s.secret = b
	return b, nil
}

func (s *AuthnStore) Authenticate(email, password string) (token string, err error) {
	ok := s.checkPassword(email, password)
	if !ok {
		return "", fmt.Errorf("email/password invalid")
	}
	sec, err := s.getSecret()
	if err != nil {
		return "", err
	}
	return createIDToken(email, sec, s.tokenDuration)
}

func (s *AuthnStore) CheckToken(token string) (claims *auth.Claims, err error) {
	sec, err := s.getSecret()
	if err != nil {
		return nil, err
	}
	return validateIDToken(token, sec)
}

func (s *AuthnStore) Flush() error {
	f, err := os.OpenFile(filepath.Join(s.rootDir, "authn.csv"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	return w.WriteAll(s.sl)
}

func (s *AuthnStore) search(email string) int {
	return sort.Search(s.Len(), func(i int) bool {
		return s.sl[i][0] >= email
	})
}

func (s *AuthnStore) findUserIndex(email string) int {
	ind := s.search(email)
	if ind < s.Len() && s.sl[ind][0] == email {
		return ind
	}
	return -1
}

func (s *AuthnStore) SetPassword(email, password string) error {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	i := s.search(email)
	if i < s.Len() {
		s.sl = append(s.sl[:i+1], s.sl[i:]...)
		s.sl[i] = []string{email, string(passwordHash)}
	} else {
		s.sl = append(s.sl, []string{email, string(passwordHash)})
	}
	return nil
}

func (s *AuthnStore) checkPassword(email, password string) bool {
	if i := s.findUserIndex(email); i != -1 {
		return bcrypt.CompareHashAndPassword([]byte(s.sl[i][1]), []byte(password)) == nil
	}
	return false
}

func (s *AuthnStore) RemoveUser(email string) error {
	if i := s.findUserIndex(email); i != -1 {
		s.sl = append(s.sl[:i], s.sl[i+1:]...)
		return nil
	}
	return nil
}
