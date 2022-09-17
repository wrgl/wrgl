// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pckhoi/uma/pkg/rp"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/credentials"
)

const umaClientID = "wrgl"

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

type deviceFlowAuthServer struct {
	issuer         string
	deviceEndpoint string
	tokenEndpoint  string
	ticket         string
}

func postForm(cmd *cobra.Command, path string, form url.Values, respData interface{}) (err error) {
	cli := GetClient(cmd.Context())
	r, err := http.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	if err != nil {
		return
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := cli.Do(r)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%d %s from %s: %s", resp.StatusCode, resp.Status, path, string(b))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		return fmt.Errorf("unrecognized content type: %q", ct)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return json.Unmarshal(b, respData)
}

func (s *deviceFlowAuthServer) Authenticate(cmd *cobra.Command, clientID string) (accessToken string, err error) {
	form := url.Values{}
	form.Set("client_id", clientID)
	dcResp := &DeviceCodeResponse{}
	if err = postForm(cmd, s.deviceEndpoint, form, dcResp); err != nil {
		return
	}

	cmd.Printf("Visit %s in your browser and enter user code %q to login\n", dcResp.VerificationURI, dcResp.UserCode)

	form = url.Values{}
	form.Set("client_id", clientID)
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	form.Set("device_code", dcResp.DeviceCode)
	tokResp := &TokenResponse{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(dcResp.ExpiresIn)*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Duration(dcResp.Interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("login timeout. Last error: %v", err)
			return
		case <-ticker.C:
			if err = postForm(cmd, s.tokenEndpoint, form, tokResp); err == nil {
				cmd.Printf("")
				return tokResp.AccessToken, nil
			}
		}
	}
}

func (s *deviceFlowAuthServer) RequestRPT(cmd *cobra.Command, accessToken, clientID, oldRPT string) (rpt string, err error) {
	kc, err := rp.NewKeycloakClient(s.issuer, clientID, "", GetClient(cmd.Context()))
	if err != nil {
		return "", err
	}
	return kc.RequestRPT(accessToken, rp.RPTRequest{
		Ticket: s.ticket,
		RPT:    oldRPT,
	})
}

type openidConfig struct {
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint,omitempty"`
	TokenEndpoint               string `json:"token_endpoint,omitempty"`
}

func errUnanticipatedResponse(resp *http.Response) error {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("unanticipated response %d (%s) %s", resp.StatusCode, resp.Header.Get("Content-Type"), string(b))
}

func decodeJSONResponse(resp *http.Response, obj any) error {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, obj)
}

func discoverAuthServer(cmd *cobra.Command, asURI, ticket string) (*deviceFlowAuthServer, error) {
	client := GetClient(cmd.Context())
	resp, err := client.Get(asURI + "/.well-known/openid-configuration")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 || !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		return nil, errUnanticipatedResponse(resp)
	}
	cfg := &openidConfig{}
	if err := decodeJSONResponse(resp, cfg); err != nil {
		return nil, err
	}
	if cfg.DeviceAuthorizationEndpoint == "" {
		return nil, fmt.Errorf("authorization server does not support device grant flow")
	}
	return &deviceFlowAuthServer{
		issuer:         asURI,
		deviceEndpoint: cfg.DeviceAuthorizationEndpoint,
		tokenEndpoint:  cfg.TokenEndpoint,
		ticket:         ticket,
	}, nil
}

func handleUMATicket(cmd *cobra.Command, cs *credentials.Store, repoURI url.URL, asURI, ticket, oldRPT string) (rpt string, err error) {
	s, err := discoverAuthServer(cmd, asURI, ticket)
	if err != nil {
		return
	}
	accessToken, err := s.Authenticate(cmd, umaClientID)
	if err != nil {
		return
	}
	rpt, err = s.RequestRPT(cmd, accessToken, umaClientID, oldRPT)
	if err != nil {
		return
	}
	cs.Set(repoURI, rpt)
	if err = cs.Flush(); err != nil {
		return
	}
	cmd.Printf("Saved credentials to %s\n", cs.Path())
	return
}
