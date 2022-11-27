// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conf

import (
	"time"
)

const (
	DefaultTransactionTTL Duration = Duration(time.Hour * 24 * 30)
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

	// Delimiter is the CSV delimiter of File. Defaults to comma.
	Delimiter rune `yaml:"delimiter,omitempty" json:"delimiter,omitempty"`
}

type AuthKeycloak struct {
	Issuer       string `json:"issuer,omitempty" yaml:"issuer,omitempty"`
	ClientID     string `json:"clientId,omitempty" yaml:"clientId,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty" yaml:"clientSecret,omitempty"`
}

type Auth struct {
	// AnonymousRead when set to true, gives anonymous users "read" scope
	AnonymousRead bool `yaml:"anonymousRead,omitempty" json:"anonymousRead,omitempty"`

	// Keycloak contains Keycloak credentials of this repo as a resource server
	Keycloak *AuthKeycloak `json:"keycloak,omitempty" yaml:"keycloak,omitempty"`

	// RepositoryName is the UMA resource name of this repo
	RepositoryName string `json:"repositoryName,omitempty" yaml:"repositoryName,omitempty"`
}

type Pack struct {
	// MaxFileSize is the maximum packfile size in bytes. Note that unlike in Git, pack format
	// is only used as a transport format during fetch and push. This size is pre-compression.
	MaxFileSize uint64 `yaml:"maxFileSize,omitempty" json:"maxFileSize,omitempty"`
}

type FastForward string

func (s FastForward) String() string {
	return string(s)
}

const (
	FF_Default FastForward = ""
	FF_Only    FastForward = "only"
	FF_Never   FastForward = "never"
)

type Merge struct {
	// FastForward controls how merge operations create new commit. By default, Wrgl will not
	// create an extra merge commit when merging a commit that is a descendant of the latest commit.
	// Instead, the tip of the branch is fast-forwarded. When set to FF_Never, this tells Wrgl
	// to always create an extra merge commit in such a case. When set to FF_Only, this tells
	// Wrgl to allow only fast-forward merges.
	FastForward FastForward `yaml:"fastForward,omitempty" json:"fastForward,omitempty"`
}

type Config struct {
	User    *User              `yaml:"user,omitempty" json:"user,omitempty"`
	Remote  map[string]*Remote `yaml:"remote,omitempty" json:"remote,omitempty"`
	Receive *Receive           `yaml:"receive,omitempty" json:"receive,omitempty"`
	Branch  map[string]*Branch `yaml:"branch,omitempty" json:"branch,omitempty"`
	Auth    *Auth              `yaml:"auth,omitempty" json:"auth,omitempty"`
	Pack    *Pack              `yaml:"pack,omitempty" json:"pack,omitempty"`
	Merge   *Merge             `yaml:"merge,omitempty" json:"merge,omitempty"`
	BaseURL string             `json:"baseUrl,omitempty" yaml:"baseUrl,omitempty"`

	// TransactionTTL is the maximum amount of time a transaction can exist before
	// being garbage-collected. Defaults to 30 days
	TransactionTTL Duration `yaml:"transactionTTL,omitempty" json:"transactionTTL,omitempty"`

	// wrgld send events to Webhooks according to registered event types
	Webhooks []Webhook `yaml:"webhooks,omitempty" json:"webhooks,omitempty"`
}

func (c *Config) MaxPackFileSize() uint64 {
	if c.Pack != nil {
		return c.Pack.MaxFileSize
	}
	return 0
}

func (c *Config) GetTransactionTTL() time.Duration {
	if c.TransactionTTL != 0 {
		return time.Duration(c.TransactionTTL)
	}
	return time.Duration(DefaultTransactionTTL)
}

func (c *Config) MergeFastForward() FastForward {
	if c.Merge != nil && c.Merge.FastForward != "" {
		return c.Merge.FastForward
	}
	return FF_Default
}
