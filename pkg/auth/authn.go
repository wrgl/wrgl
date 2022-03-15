// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import "net/http"

type AuthnStore interface {
	SetName(email, name string) error
	SetPassword(email, password string) error
	Authenticate(email, password string) (token string, err error)
	CheckToken(r *http.Request, token string) (*http.Request, *Claims, error)
	RemoveUser(email string) error
	// ListUsers returns list of users, each user is the string slice []string{email, name}
	ListUsers() (users [][]string, err error)
	Exist(email string) bool
	Flush() error
}
