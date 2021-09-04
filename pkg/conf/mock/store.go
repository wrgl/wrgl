package confmock

import "github.com/wrgl/core/pkg/conf"

type Store struct {
	c conf.Config
}

func (s *Store) Open() (*conf.Config, error) {
	return &s.c, nil
}

func (s *Store) Save(c *conf.Config) error {
	s.c = *c
	return nil
}
