// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package auth

type AuthnStore interface {
	SetPassword(email, password string) error
	Authenticate(email, password string) (token string, err error)
	CheckToken(token string) (claims *Claims, err error)
	RemoveUser(email string) error
	Flush() error
}
