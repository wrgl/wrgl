// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type URL struct {
	url.URL
}

func (u *URL) MarshalYAML() (interface{}, error) {
	b, err := u.URL.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (u *URL) UnmarshalYAML(value *yaml.Node) error {
	return u.URL.UnmarshalBinary([]byte(value.Value))
}

type Credentials struct {
	URI   *URL   `yaml:"host,omitempty"`
	Token string `yaml:"token,omitempty"`
}

type Store struct {
	fp    string
	creds []*Credentials
}

func NewStore() (*Store, error) {
	fp := credsLocation()
	s := &Store{
		fp: fp,
	}
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, &s.creds); err != nil {
			return nil, err
		}
		sort.Sort(s)
	} else {
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Path() string {
	return s.fp
}

func (s *Store) Len() int {
	return len(s.creds)
}

func (s *Store) Less(i, j int) bool {
	ui, uj := s.creds[i].URI, s.creds[j].URI
	return ui.Host > uj.Host || (ui.Host == uj.Host && ui.Path > uj.Path)
}

func (s *Store) Swap(i, j int) {
	s.creds[i], s.creds[j] = s.creds[j], s.creds[i]
}

func (s *Store) URIs() []url.URL {
	sl := make([]url.URL, s.Len())
	for i, cred := range s.creds {
		sl[i] = cred.URI.URL
	}
	return sl
}

func (s *Store) search(uri url.URL) int {
	return sort.Search(len(s.creds), func(i int) bool {
		u := s.creds[i].URI
		return u.Host < uri.Host || (u.Host == uri.Host && u.Path <= uri.Path)
	})
}

func (s *Store) indexOfURI(uri url.URL) int {
	i := s.search(uri)
	if i < s.Len() && s.creds[i].URI.String() == uri.String() {
		return i
	}
	return -1
}

func (s *Store) searchHost(uri url.URL) (int, int) {
	start := sort.Search(len(s.creds), func(i int) bool {
		return s.creds[i].URI.Host <= uri.Host
	})
	end := sort.Search(len(s.creds), func(i int) bool {
		return s.creds[i].URI.Host < uri.Host
	})
	return start, end
}

func ensureTrailingSlash(p string) string {
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}

func (s *Store) indexOfMatchingPrefix(uri url.URL) int {
	start, end := s.searchHost(uri)
	for i := start; i < end; i++ {
		if strings.HasPrefix(
			ensureTrailingSlash(uri.Path),
			ensureTrailingSlash(s.creds[i].URI.Path),
		) {
			return i
		}
	}
	return -1
}

func (s *Store) Set(uri url.URL, token string) {
	i := s.search(uri)
	if i < s.Len() && s.creds[i].URI.String() == uri.String() {
		s.creds[i].Token = token
		return
	}
	tok := &Credentials{
		URI:   &URL{uri},
		Token: token,
	}
	if i < s.Len() {
		s.creds = append(s.creds[:i+1], s.creds[i:]...)
		s.creds[i] = tok
	} else {
		s.creds = append(s.creds, tok)
	}
}

// GetTokenMatching returns the (uri, token) pair with longest path that
// is a prefix of input uri.
func (s *Store) GetTokenMatching(uri url.URL) (*url.URL, string) {
	if i := s.indexOfMatchingPrefix(uri); i != -1 {
		return &s.creds[i].URI.URL, s.creds[i].Token
	}
	return nil, ""
}

func (s *Store) Delete(uri url.URL) bool {
	if i := s.indexOfURI(uri); i != -1 {
		if i < s.Len()-1 {
			s.creds = append(s.creds[:i], s.creds[i+1:]...)
		} else {
			s.creds = s.creds[:i]
		}
		return true
	}
	return false
}

func (s *Store) Flush() error {
	f, err := os.OpenFile(s.fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := yaml.Marshal(s.creds)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}
