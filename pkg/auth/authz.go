// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package auth

import "net/http"

const (
	Anyone               = "anyone"
	ScopeRepoRead        = "repo.read"
	ScopeRepoWrite       = "repo.write"
	ScopeRepoReadConfig  = "repo.readConfig"
	ScopeRepoWriteConfig = "repo.writeConfig"
)

type AuthzStore interface {
	AddPolicy(email, scope string) error
	RemovePolicy(email, scope string) error
	Authorized(r *http.Request, email, scope string) (bool, error)
	ListPolicies(email string) (scopes []string, err error)
	Flush() error
}
