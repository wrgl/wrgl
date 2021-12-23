// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/google/uuid"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/conf"
)

type CreateRepoRequest struct {
	Name   string      `json:"name"`
	Config conf.Config `json:"config"`
	Public *bool       `json:"public,omitempty"`
}

type CreateRepoResponse struct {
	ID uuid.UUID `json:"id"`
}

type Repo struct {
	Name   string     `json:"name"`
	ID     *uuid.UUID `json:"id,omitempty"`
	Public bool       `json:"isPublic"`
	Scopes []string   `json:"scopes"`
}

type ListReposResponse struct {
	Repos []Repo `json:"repos"`
	Count int    `json:"count"`
}

func parseJSON(resp *http.Response, target interface{}) error {
	if resp.StatusCode != http.StatusOK {
		return apiclient.NewHTTPError(resp)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

func CreateRepo(tok, username string, crr *CreateRepoRequest) (createResp *CreateRepoResponse, err error) {
	b, err := json.Marshal(crr)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/users/%s/repos/", APIRoot, username), bytes.NewReader(b))
	if err != nil {
		return
	}
	setAuthToken(req, tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	createResp = &CreateRepoResponse{}
	if err = parseJSON(resp, createResp); err != nil {
		return
	}
	return createResp, nil
}

func ListRepos(tok, username string, offset int) (listResp *ListReposResponse, err error) {
	path := fmt.Sprintf("%s/users/%s/repos/", APIRoot, username)
	if offset > 0 {
		path = fmt.Sprintf("%s?offset=%d", path, offset)
	}
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	setAuthToken(req, tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	listResp = &ListReposResponse{}
	if err = parseJSON(resp, listResp); err != nil {
		return
	}
	return listResp, nil
}

func DeleteRepo(tok, username, reponame string) (err error) {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/users/%s/repos/%s/", APIRoot, username, reponame), nil)
	if err != nil {
		return
	}
	setAuthToken(req, tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		return apiclient.NewHTTPError(resp)
	}
	return nil
}
