package conffs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/wrgl/core/pkg/conf"
	"gopkg.in/yaml.v3"
)

type Source int

const (
	UnspecifiedSource Source = iota
	FileSource
	LocalSource
	GlobalSource
	SystemSource
	AggregateSource
)

type Store struct {
	rootDir string
	source  Source
	fp      string
}

func NewStore(rootDir string, source Source, fp string) *Store {
	if fp != "" {
		source = FileSource
	}
	return &Store{
		rootDir: rootDir,
		source:  source,
		fp:      fp,
	}
}

func (s *Store) readConfig(fp string) (*conf.Config, error) {
	c := &conf.Config{}
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		err = yaml.Unmarshal(b, c)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return c, nil
}

func (s *Store) Open() (*conf.Config, error) {
	if s.source == AggregateSource {
		return s.aggregateConfig()
	}
	fp, err := s.path()
	if err != nil {
		return nil, err
	}
	return s.readConfig(fp)
}

func (s *Store) Save(c *conf.Config) error {
	if s.source == AggregateSource {
		return fmt.Errorf("attempt to save aggregated config")
	}
	fp, err := s.path()
	if err != nil {
		return err
	}
	if fp == "" {
		return fmt.Errorf("empty config path")
	}
	err = os.MkdirAll(filepath.Dir(fp), 0755)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}
