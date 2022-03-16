// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conf

import (
	"time"

	"github.com/wrgl/wrgl/pkg/slice"
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
}

type AuthType string

func (s AuthType) String() string {
	return string(s)
}

const (
	// When Auth.Type is set to ATLegacy, read & write users/permissions into flat files.
	// See `wrgl auth -h` for more info
	ATLegacy AuthType = "legacy"

	// When Auth.Type is set to ATOauth2, the wrgld server behaves like an OAuth 2.0 server while
	// relying on an external OIDC provider (most likely Keycloak) for user & permission management.
	ATOauth2 AuthType = "oauth2"
)

type AuthOIDCProvider struct {
	// Issuer is the http URI of the OIDC provider
	Issuer string `yaml:"issuer,omitempty" json:"issuer,omitempty"`

	// ClientID is the registered client id of the wrgld server
	ClientID string `yaml:"clientID,omitempty" json:"clientID,omitempty"`

	// ClientSecret is the registered client secret of the wrgld server
	ClientSecret string `yaml:"clientSecret,omitempty" json:"clientSecret,omitempty"`

	// Address is the address of the wrgld server that the OIDC provider can reach.
	// It will be used when constructing callback URI during authorization flow.
	Address string `yaml:"address,omitempty" json:"address,omitempty"`
}

// AuthClient is the config of a public client of wrgld server
type AuthClient struct {
	// ID is the client id of a public client. To authorize against wrgld, the provided
	// "client_id" param must match one of the registered ids.
	ID string `yaml:"id,omitempty" json:"id,omitempty"`

	// RedirectURIs are glob patterns that "redirect_uri" will be compared against.
	// See https://github.com/gobwas/glob for supported format
	RedirectURIs []string `yaml:"redirectURIs,omitempty" json:"redirectURIs,omitempty"`
}

type AuthOAuth2 struct {
	// OIDCProvider contains configurations of OIDC provider
	OIDCProvider *AuthOIDCProvider `yaml:"oidcProvider,omitempty" json:"oidcProvider,omitempty"`

	// Clients contains OAuth 2 client configurations when Type is ATOauth2
	Clients []AuthClient `yaml:"clients,omitempty" json:"clients,omitempty"`
}

type Auth struct {
	// TokenDuration specifies how long before a JWT token given by the `/authenticate/`
	// endpoint of Wrgld expire. This is a string in the format "72h3m0.5s". Tokens last for
	// 90 days by default.
	TokenDuration *Duration `yaml:"tokenDuration,omitempty" json:"tokenDuration,omitempty"`

	// Types specifies how authorization work with wrgld API
	Type AuthType `yaml:"type,omitempty" json:"type,omitempty"`

	// AnonymousRead when set to true, allow anonymous users (users without an access token)
	// to be assigned auth.ScopeRepoRead scope, thus are able to read the data from this repo.
	AnonymousRead bool `yaml:"anonymousRead,omitempty" json:"anonymousRead,omitempty"`

	// OAuth2 configurations that is effective when Type is ATOauth2
	OAuth2 *AuthOAuth2 `yaml:"oauth2,omitempty" json:"oauth2,omitempty"`
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

	// TransactionTTL is the maximum amount of time a transaction can exist before
	// being garbage-collected. Defaults to 30 days
	TransactionTTL Duration `yaml:"transactionTTL,omitempty" json:"transactionTTL,omitempty"`
}

func (c *Config) TokenDuration() time.Duration {
	if c.Auth != nil {
		return time.Duration(*c.Auth.TokenDuration)
	}
	return 0
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

func (c *Config) IsBranchPrimaryKeyEqual(branchName string, primaryKey []string) bool {
	if c.Branch != nil {
		if branch, ok := c.Branch[branchName]; ok {
			return slice.StringSliceEqual(branch.PrimaryKey, primaryKey)
		}
	}
	return len(primaryKey) == 0
}

func (c *Config) MergeFastForward() FastForward {
	if c.Merge != nil && c.Merge.FastForward != "" {
		return c.Merge.FastForward
	}
	return FF_Default
}
