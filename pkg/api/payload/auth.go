// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

type AuthenticateRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthenticateResponse struct {
	IDToken string `json:"idToken"`
}
