package flatdb

import (
	"encoding/csv"
	"os"
	"sort"

	"golang.org/x/crypto/bcrypt"
)

type AuthnStore struct {
	sl [][]string
	fp string
}

func NewAuthnStore(fp string) (s *AuthnStore, err error) {
	s = &AuthnStore{
		fp: fp,
	}
	f, err := os.Open(fp)
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

func (s *AuthnStore) Less(i, j int) bool {
	return s.sl[i][0] < s.sl[j][0]
}

func (s *AuthnStore) Swap(i, j int) {
	s.sl[i], s.sl[j] = s.sl[j], s.sl[i]
}

func (s *AuthnStore) save() error {
	f, err := os.Create(s.fp)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	return w.WriteAll(s.sl)
}

func (s *AuthnStore) findUserIndex(email string) int {
	n := len(s.sl)
	ind := sort.Search(n, func(i int) bool {
		return s.sl[i][0] >= email
	})
	if ind < n && s.sl[ind][0] == email {
		return ind
	}
	return -1
}

func (s *AuthnStore) SetPassword(email, password string) error {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	s.sl = append(s.sl, []string{email, string(passwordHash)})
	sort.Sort(s)
	return s.save()
}

func (s *AuthnStore) CheckPassword(email, password string) bool {
	if i := s.findUserIndex(email); i != -1 {
		return bcrypt.CompareHashAndPassword([]byte(s.sl[i][1]), []byte(password)) == nil
	}
	return false
}

func (s *AuthnStore) RemoveUser(email string) error {
	if i := s.findUserIndex(email); i != -1 {
		s.sl = append(s.sl[:i], s.sl[i+1:]...)
		return s.save()
	}
	return nil
}
