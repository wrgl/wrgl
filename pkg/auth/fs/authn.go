// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/auth/random"
	"github.com/wrgl/wrgl/pkg/local"
	"golang.org/x/crypto/bcrypt"
)

const DefaultTokenDuration time.Duration = time.Hour * 24 * 90 // 90 days

type AuthnStore struct {
	// sl is all current data, each element is a list of {email, name, passwordHash}
	sl            [][]string
	rootDir       string
	secret        []byte
	tokenDuration time.Duration
	watcher       *fsnotify.Watcher
}

func NewAuthnStore(rd *local.RepoDir, tokenDuration time.Duration) (s *AuthnStore, err error) {
	if tokenDuration == 0 {
		tokenDuration = DefaultTokenDuration
	}
	s = &AuthnStore{
		rootDir:       rd.FullPath,
		tokenDuration: tokenDuration,
	}
	if err = s.read(); err != nil {
		return nil, err
	}
	s.watcher, err = rd.Watcher()
	if err != nil {
		return nil, err
	}
	go s.watch()
	return s, nil
}

func (s *AuthnStore) Filepath() string {
	return filepath.Join(s.rootDir, "authn.csv")
}

func (s *AuthnStore) read() error {
	fp := s.Filepath()
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		r := csv.NewReader(f)
		s.sl, err = r.ReadAll()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *AuthnStore) watch() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				if event.Name == s.Filepath() {
					if err := s.read(); err != nil {
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
	i, ok := s.checkPassword(email, password)
	if !ok {
		return "", fmt.Errorf("email/password invalid")
	}
	sec, err := s.getSecret()
	if err != nil {
		return "", err
	}
	name := s.sl[i][1]
	return createIDToken(email, name, sec, s.tokenDuration)
}

func (s *AuthnStore) CheckToken(r *http.Request, token string) (*http.Request, *auth.Claims, error) {
	sec, err := s.getSecret()
	if err != nil {
		return nil, nil, err
	}
	claims, err := validateIDToken(token, sec)
	if err != nil {
		return nil, nil, err
	}
	return r, claims, nil
}

func (s *AuthnStore) Flush() error {
	f, err := os.OpenFile(s.Filepath(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.WriteAll(s.sl); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return f.Close()
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
	if i < s.Len() && s.sl[i][0] == email {
		s.sl[i][2] = string(passwordHash)
		return nil
	}
	if i < s.Len() {
		s.sl = append(s.sl[:i+1], s.sl[i:]...)
		s.sl[i] = []string{email, "", string(passwordHash)}
	} else {
		s.sl = append(s.sl, []string{email, "", string(passwordHash)})
	}
	return nil
}

func (s *AuthnStore) SetName(email, name string) error {
	i := s.search(email)
	if i < s.Len() && s.sl[i][0] == email {
		s.sl[i][1] = name
		return nil
	}
	if i < s.Len() {
		s.sl = append(s.sl[:i+1], s.sl[i:]...)
		s.sl[i] = []string{email, name, ""}
	} else {
		s.sl = append(s.sl, []string{email, name, ""})
	}
	return nil
}

func (s *AuthnStore) checkPassword(email, password string) (int, bool) {
	if i := s.findUserIndex(email); i != -1 {
		return i, bcrypt.CompareHashAndPassword([]byte(s.sl[i][2]), []byte(password)) == nil
	}
	return 0, false
}

func (s *AuthnStore) RemoveUser(email string) error {
	if i := s.findUserIndex(email); i != -1 {
		s.sl = append(s.sl[:i], s.sl[i+1:]...)
		return nil
	}
	return nil
}

func (s *AuthnStore) ListUsers() (users [][]string, err error) {
	users = make([][]string, s.Len())
	for i, entry := range s.sl {
		users[i] = []string{entry[0], entry[1]}
	}
	return users, nil
}

func (s *AuthnStore) Exist(email string) bool {
	return s.findUserIndex(email) != -1
}

// InternalState returns internal state as a CSV string for debugging purpose
func (s *AuthnStore) InternalState() string {
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	w.WriteAll(s.sl)
	w.Flush()
	return buf.String()
}
