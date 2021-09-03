package credentials

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Store struct {
	fp     string
	tokens map[string]string
}

func NewStore() (*Store, error) {
	fp := credsLocation()
	s := &Store{
		fp:     fp,
		tokens: map[string]string{},
	}
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, s.tokens); err != nil {
			return nil, err
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Set(remoteURL, token string) {
	s.tokens[remoteURL] = token
}

func (s *Store) Get(remoteURL string) (string, bool) {
	t, ok := s.tokens[remoteURL]
	return t, ok
}

func (s *Store) Delete(remoteURL string) {
	delete(s.tokens, remoteURL)
}

func (s *Store) Flush() error {
	f, err := os.OpenFile(s.fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := yaml.Marshal(s.tokens)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}
