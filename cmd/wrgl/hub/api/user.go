// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package api

import (
	"net/http"

	"github.com/google/uuid"
)

const APIRoot = "https://hub.wrgl.co/api"

type User struct {
	ID             uuid.UUID `json:"id"`
	Email          string    `json:"email"`
	Name           string    `json:"name"`
	Avatar         string    `json:"avatar"`
	Username       string    `json:"username"`
	IsOrganization bool      `json:"isOrganization"`
	Organizations  []User    `json:"orgs"`
	Members        []User    `json:"members"`
	CanCommit      bool      `json:"canCommit"`
}

func setAuthToken(req *http.Request, tok string) {
	req.Header.Set("Authorization", "Bearer "+tok)
}

func GetMe(tok string) (user *User, err error) {
	req, err := http.NewRequest(http.MethodGet, APIRoot+"/me/", nil)
	if err != nil {
		return
	}
	setAuthToken(req, tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	user = &User{}
	if err = parseJSON(resp, user); err != nil {
		return
	}
	return user, nil
}
