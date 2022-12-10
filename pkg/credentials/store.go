// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

type RepoCredentials struct {
	RelyingPartyToken string `yaml:"relyingPartyToken,omitempty"`
}

type AuthServerCredentials struct {
	AccessToken  string `yaml:"accessToken,omitempty"`
	RefreshToken string `yaml:"refreshToken,omitempty"`
}

type Credentials struct {
	Repos       map[string]*RepoCredentials       `yaml:"repos,omitempty"`
	AuthServers map[string]*AuthServerCredentials `yaml:"authServers,omitempty"`
}

type Store struct {
	fp    string
	creds *Credentials
}

func NewStore() (*Store, error) {
	fp := credsLocation()
	s := &Store{
		fp: fp,
		creds: &Credentials{
			AuthServers: map[string]*AuthServerCredentials{},
			Repos:       map[string]*RepoCredentials{},
		},
	}
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		yaml.Unmarshal(b, s.creds)
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

func (s *Store) RepoURIs() ([]url.URL, error) {
	sl := make([]url.URL, 0, len(s.creds.Repos))
	for s := range s.creds.Repos {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		sl = append(sl, *u)
	}
	sort.Slice(sl, func(i, j int) bool {
		if sl[i].Host < sl[j].Host {
			return true
		} else if sl[i].Host > sl[j].Host {
			return false
		} else if sl[i].Path < sl[j].Path {
			return true
		} else if sl[i].Path > sl[j].Path {
			return false
		}
		return false
	})
	return sl, nil
}

func (s *Store) SetAccessToken(asURI url.URL, accessToken, refreshToken string) {
	s.creds.AuthServers[asURI.String()] = &AuthServerCredentials{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}
}

func (s *Store) SetRPT(repoURI url.URL, rpt string) {
	s.creds.Repos[repoURI.String()] = &RepoCredentials{
		RelyingPartyToken: rpt,
	}
}

func (s *Store) GetAccessToken(asURI url.URL) string {
	if c, ok := s.creds.AuthServers[asURI.String()]; ok {
		return c.AccessToken
	}
	return ""
}

func (s *Store) GetRefreshToken(asURI url.URL) string {
	if c, ok := s.creds.AuthServers[asURI.String()]; ok {
		return c.RefreshToken
	}
	return ""
}

func (s *Store) GetRPT(repoURI url.URL) string {
	if c, ok := s.creds.Repos[repoURI.String()]; ok {
		return c.RelyingPartyToken
	}
	return ""
}

func (s *Store) DeleteRepo(repoURI url.URL) bool {
	if _, ok := s.creds.Repos[repoURI.String()]; ok {
		delete(s.creds.Repos, repoURI.String())
		return true
	}
	return false
}

func (s *Store) DeleteAuthServer(asURI url.URL) bool {
	if _, ok := s.creds.AuthServers[asURI.String()]; ok {
		delete(s.creds.AuthServers, asURI.String())
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
