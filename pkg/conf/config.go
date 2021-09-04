// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

import "time"

type User struct {
	Email string `yaml:"email,omitempty" json:"email,omitempty"`
	Name  string `yaml:"name,omitempty" json:"name,omitempty"`
}

type Receive struct {
	DenyNonFastForwards *bool `yaml:"denyNonFastForwards,omitempty" json:"denyNonFastForwards,omitempty"`
	DenyDeletes         *bool `yaml:"denyDeletes,omitempty" json:"denyDeletes,omitempty"`
}

type Branch struct {
	Remote string `yaml:"remote,omitempty" json:"remote,omitempty"`
	Merge  string `yaml:"merge,omitempty" json:"merge,omitempty"`
}

type Auth struct {
	TokenDuration time.Duration `yaml:"tokenDuration,omitempty" json:"tokenDuration,omitempty"`
}

type Pack struct {
	// MaxFileSize is the maximum pack file size in bytes. Note that unlike in Git, pack format
	// is only used as a transport format during fetch and push. This size is pre-compression.
	MaxFileSize uint64 `yaml:"maxFileSize,omitempty" json:"maxFileSize,omitempty"`
}

type Config struct {
	User    *User              `yaml:"user,omitempty" json:"user,omitempty"`
	Remote  map[string]*Remote `yaml:"remote,omitempty" json:"remote,omitempty"`
	Receive *Receive           `yaml:"receive,omitempty" json:"receive,omitempty"`
	Branch  map[string]*Branch `yaml:"branch,omitempty" json:"branch,omitempty"`
	Auth    *Auth              `yaml:"auth,omitempty" json:"auth,omitempty"`
	Pack    *Pack              `yaml:"pack,omitempty" json:"pack,omitempty"`
}
