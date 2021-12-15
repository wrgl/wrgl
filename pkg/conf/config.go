// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

import (
	"time"

	"github.com/wrgl/wrgl/pkg/slice"
)

type User struct {
	// Email is the current user's email. Just like
	// with Git, most operations that alter data record the user's
	// email. Unlike Git however, email is always required.
	Email string `yaml:"email,omitempty" json:"email,omitempty"`

	// Name is the current user's name.
	Name string `yaml:"name,omitempty" json:"name,omitempty"`
}

type Receive struct {
	// DenyNonFastForwards, when set to `true`, during push, Wrgld denies all updates that
	// are not fast-forwards.
	DenyNonFastForwards *bool `yaml:"denyNonFastForwards,omitempty" json:"denyNonFastForwards,omitempty"`

	// DenyDeletes, when set to `true`, during push, Wrgld denies all reference deletes.
	DenyDeletes *bool `yaml:"denyDeletes,omitempty" json:"denyDeletes,omitempty"`
}

type Branch struct {
	// Remote is the upstream remote of this branch. When both this setting and Merge is set,
	// user can run `wrgl pull <branch>` without specifying remote and refspec.
	Remote string `yaml:"remote,omitempty" json:"remote,omitempty"`

	// Merge is the upstream destination of this branch. When both this setting and Remote is
	// set, user can run `wrgl pull <branch>` without specifying remote and refspec.
	Merge string `yaml:"merge,omitempty" json:"merge,omitempty"`

	// File is the path of a file to diff against, or commit to this branch if no file is specified.
	File string `yaml:"file,omitempty" json:"file,omitempty"`

	// PrimaryKey is the primary key used in addition to branch.file during diff or commit if
	// no file is specified.
	PrimaryKey []string `yaml:"primaryKey,omitempty" json:"primaryKey,omitempty"`
}

type Auth struct {
	// TokenDuration specifies how long before a JWT token given by the `/authenticate/`
	// endpoint of Wrgld expire. This is a string in the format "72h3m0.5s". Tokens last for
	// 90 days by default.
	TokenDuration time.Duration `yaml:"tokenDuration,omitempty" json:"tokenDuration,omitempty"`
}

type Pack struct {
	// MaxFileSize is the maximum packfile size in bytes. Note that unlike in Git, pack format
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

func (c *Config) TokenDuration() time.Duration {
	if c.Auth != nil {
		return c.Auth.TokenDuration
	}
	return 0
}

func (c *Config) MaxPackFileSize() uint64 {
	if c.Pack != nil {
		return c.Pack.MaxFileSize
	}
	return 0
}

func (c *Config) IsBranchPrimaryKeyEqual(branchName string, primaryKey []string) bool {
	if c.Branch != nil {
		if branch, ok := c.Branch[branchName]; ok {
			return slice.StringSliceEqual(branch.PrimaryKey, primaryKey)
		}
	}
	return len(primaryKey) == 0
}
