// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import "github.com/golang-jwt/jwt"

type Claims struct {
	jwt.StandardClaims
	Email string `json:"email,omitempty"`
	Name  string `json:"name,omitempty"`
}
