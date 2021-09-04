// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package auth

const (
	ScopeRead        = "read"
	ScopeWrite       = "write"
	ScopeReadConfig  = "readConfig"
	ScopeWriteConfig = "writeConfig"
)

type AuthzStore interface {
	AddPolicy(email, scope string) error
	RemovePolicy(email, scope string) error
	Authorized(email, scope string) (bool, error)
	Flush() error
}
