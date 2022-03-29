// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func authenticateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "authenticate { REMOTE_URI | REMOTE_NAME }",
		Short: "Authenticate for one or more remotes with email/password.",
		Long:  "Authenticate for one or more remotes with email/password and save credentials for future use. If REMOTE_NAME is given, then login and save credentials for that remote. If REMOTE_URI is given, login at REMOTE_URI/authenticate/ and save credentials for all remotes that have REMOTE_URI as prefix.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "authenticate for origin",
				Line:    "wrgl credentials authenticate origin",
			},
			{
				Comment: "authenticate for all repositories on wrgl hub",
				Line:    "wrgl credentials authenticate https://hub.wrgl.co/api",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cfs := conffs.NewStore(dir, conffs.LocalSource, "")
			c, err := cfs.Open()
			if err != nil {
				return err
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			if v, ok := c.Remote[args[0]]; ok {
				return getCredentials(cmd, cs, v.URL)
			} else {
				return getCredentials(cmd, cs, args[0])
			}
		},
	}
	return cmd
}

func detectAuthType(cmd *cobra.Command, uriStr string) (authType conf.AuthType, err error) {
	r, err := http.NewRequest(http.MethodPost, uriStr+"/oauth2/devicecode/", nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest {
		cmd.Printf("Detected auth type %q\n", conf.ATOauth2)
		return conf.ATOauth2, nil
	} else if resp.StatusCode == http.StatusNotFound {
		cmd.Printf("Detected auth type %q\n", conf.ATLegacy)
		return conf.ATLegacy, nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return "", fmt.Errorf("unaticipated response from %s/oauth2/devicecode/ - %d: %s", uriStr, resp.StatusCode, string(b))
}

type AuthenticateRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthenticateResponse struct {
	IDToken string `json:"idToken"`
}

func authenticate(uri, email, password string) (token string, err error) {
	b, err := json.Marshal(&AuthenticateRequest{
		Email:    email,
		Password: password,
	})
	if err != nil {
		return
	}
	r, err := http.NewRequest(http.MethodPost, uri+"/authenticate/", bytes.NewReader(b))
	if err != nil {
		return
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		return "", fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	ar := &AuthenticateResponse{}
	err = json.Unmarshal(b, ar)
	if err != nil {
		return
	}
	return ar.IDToken, nil
}

func getLegacyCredentials(cmd *cobra.Command, uriS string) (token string, err error) {
	cmd.Printf("Enter your email and password for %s.\n", uriS)
	reader := bufio.NewReader(cmd.InOrStdin())
	cmd.Print("Email: ")
	email, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	email = strings.TrimSpace(email)
	password, err := utils.PromptForPassword(cmd)
	if err != nil {
		return "", err
	}
	token, err = authenticate(uriS, email, password)
	if err != nil {
		return "", err
	}
	return
}

func postForm(path string, form url.Values, respData interface{}) (err error) {
	r, err := http.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	if err != nil {
		return
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%d %s from %s: %s", resp.StatusCode, resp.Status, path, string(b))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		return fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return json.Unmarshal(b, respData)
}

type DeviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
}

func getOauth2Credentials(cmd *cobra.Command, uriS string) (token string, err error) {
	reader := bufio.NewReader(cmd.InOrStdin())
	cmd.Printf("Enter OAuth 2 client ID for this CLI: ")
	clientID, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	clientID = strings.TrimSpace(clientID)

	form := url.Values{}
	form.Set("client_id", clientID)
	dcResp := &DeviceCodeResponse{}
	if err = postForm(uriS+"/oauth2/devicecode/", form, dcResp); err != nil {
		return
	}

	cmd.Printf("Visit %s/oauth2/device/ in your browser and enter user code %q to login\n", uriS, dcResp.UserCode)

	form = url.Values{}
	form.Set("client_id", clientID)
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	form.Set("device_code", dcResp.DeviceCode)
	tokResp := &TokenResponse{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(dcResp.ExpiresIn)*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Duration(dcResp.Interval) * time.Second)
	for {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("login timeout. Last error: %v", err)
			return
		case <-ticker.C:
			if err = postForm(uriS+"/oauth2/token/", form, tokResp); err == nil {
				cmd.Printf("")
				return tokResp.AccessToken, nil
			}
		}
	}
}

func getCredentials(cmd *cobra.Command, cs *credentials.Store, uriS string) (err error) {
	uriS = strings.TrimRight(uriS, "/")
	u, err := url.Parse(uriS)
	if err != nil {
		return
	}
	at, err := detectAuthType(cmd, uriS)
	if err != nil {
		return
	}
	var token string
	if at == conf.ATLegacy {
		token, err = getLegacyCredentials(cmd, uriS)
	} else {
		token, err = getOauth2Credentials(cmd, uriS)
	}
	if err != nil {
		return
	}
	cs.Set(*u, token)
	if err = cs.Flush(); err != nil {
		return
	}
	cmd.Printf("Saved credentials to %s\n", cs.Path())
	return nil
}
