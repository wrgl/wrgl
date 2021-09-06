// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package auth

import "net/http"

const (
	ScopeRead        = "read"
	ScopeWrite       = "write"
	ScopeReadConfig  = "readConfig"
	ScopeWriteConfig = "writeConfig"
)

type AuthzStore interface {
	AddPolicy(email, scope string) error
	RemovePolicy(email, scope string) error
	Authorized(r *http.Request, email, scope string) (bool, error)
	ListPolicies(email string) (scopes []string, err error)
	Flush() error
}
