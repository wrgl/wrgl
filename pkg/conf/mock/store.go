// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package confmock

import (
	"github.com/wrgl/wrgl/pkg/conf"
)

type Store struct {
	c conf.Config
}

func (s *Store) Open() (*conf.Config, error) {
	cfg := &conf.Config{}
	*cfg = s.c
	return cfg, nil
}

func (s *Store) Save(c *conf.Config) error {
	s.c = *c
	return nil
}
