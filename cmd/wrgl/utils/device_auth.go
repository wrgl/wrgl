// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pckhoi/uma/pkg/httputil"
	"github.com/pckhoi/uma/pkg/rp"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/credentials"
)

const oauth2ClientID = "wrgl"

type DeviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
}

type deviceFlowAuthServer struct {
	cmd            *cobra.Command
	cs             *credentials.Store
	logger         logr.Logger
	issuer         string
	issuerURL      url.URL
	deviceEndpoint string
	tokenEndpoint  string
	ticket         string
}

func discoverAuthServer(cmd *cobra.Command, cs *credentials.Store, asURI, ticket string, logger logr.Logger) (*deviceFlowAuthServer, error) {
	client := GetClient(cmd.Context())
	issuerURL, err := url.Parse(asURI)
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(asURI + "/.well-known/openid-configuration")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 || !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		return nil, httputil.NewErrUnanticipatedResponse(resp)
	}
	cfg := &openidConfig{}
	if err := decodeJSONResponse(resp, cfg); err != nil {
		return nil, err
	}
	logger.Info("discover", "openidConfig", cfg)
	if cfg.DeviceAuthorizationEndpoint == "" {
		return nil, fmt.Errorf("authorization server does not support device grant flow")
	}
	return &deviceFlowAuthServer{
		logger:         logger,
		cmd:            cmd,
		cs:             cs,
		issuer:         asURI,
		issuerURL:      *issuerURL,
		deviceEndpoint: cfg.DeviceAuthorizationEndpoint,
		tokenEndpoint:  cfg.TokenEndpoint,
		ticket:         ticket,
	}, nil
}

func postForm(cmd *cobra.Command, path string, form url.Values, respData interface{}) (err error) {
	cli := GetClient(cmd.Context())
	r, err := http.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	if err != nil {
		return
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r = r.WithContext(ctx)
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

func (s *deviceFlowAuthServer) Authenticate() (resp *TokenResponse, err error) {
	form := url.Values{}
	form.Set("client_id", oauth2ClientID)
	dcResp := &DeviceCodeResponse{}
	if err = postForm(s.cmd, s.deviceEndpoint, form, dcResp); err != nil {
		return
	}

	s.cmd.Printf("Visit %s in your browser and enter user code %q to login\n", dcResp.VerificationURI, dcResp.UserCode)

	form = url.Values{}
	form.Set("client_id", oauth2ClientID)
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
			if err = postForm(s.cmd, s.tokenEndpoint, form, tokResp); err == nil {
				s.cmd.Printf("")
				return tokResp, nil
			}
		}
	}
}

func (s *deviceFlowAuthServer) Refresh(refreshToken string) (resp *TokenResponse, err error) {
	form := url.Values{}
	form.Set("client_id", oauth2ClientID)
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	tokResp := &TokenResponse{}
	if err = postForm(s.cmd, s.tokenEndpoint, form, tokResp); err != nil {
		return nil, err
	}
	return tokResp, nil
}

func (s *deviceFlowAuthServer) GetAccessToken() (accessToken string, err error) {
	accessToken = s.cs.GetAccessToken(s.issuerURL)
	if jwtIsValid(accessToken) {
		s.logger.Info("using saved access token")
		return
	}
	if refreshToken := s.cs.GetRefreshToken(s.issuerURL); refreshToken != "" {
		resp, err := s.Refresh(refreshToken)
		if err != nil {
			return "", err
		}
		s.cs.SetAccessToken(s.issuerURL, resp.AccessToken, resp.RefreshToken)
		s.logger.Info("refreshed access token")
		return resp.AccessToken, nil
	}
	resp, err := s.Authenticate()
	if err != nil {
		return "", err
	}
	s.cs.SetAccessToken(s.issuerURL, resp.AccessToken, resp.RefreshToken)
	s.logger.Info("acquired access token via authentication")
	return resp.AccessToken, nil
}

func (s *deviceFlowAuthServer) RequestRPT(accessToken, oldRPT string) (rpt string, err error) {
	kc, err := rp.NewKeycloakClient(s.issuer, oauth2ClientID, "", GetClient(s.cmd.Context()))
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

func decodeJSONResponse(resp *http.Response, obj any) error {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, obj)
}

func handleUMATicket(cmd *cobra.Command, cs *credentials.Store, repoURI url.URL, asURI, ticket, oldRPT string, logger logr.Logger) (rpt string, err error) {
	logger = logger.WithValues(
		"repoURI", repoURI.String(),
		"asURI", asURI,
		"oldRPTExists", oldRPT != "",
	)
	s, err := discoverAuthServer(cmd, cs, asURI, ticket, logger)
	if err != nil {
		return
	}
	accessToken, err := s.GetAccessToken()
	if err != nil {
		return
	}
	rpt, err = s.RequestRPT(accessToken, oldRPT)
	if err != nil {
		var respErr *httputil.ErrUnanticipatedResponse
		if errors.As(err, &respErr) && respErr.Status == 401 {
			logger.Info("unauthorized client, renewing access token")
			cs.DeleteAuthServer(s.issuerURL)
			accessToken, err := s.GetAccessToken()
			if err != nil {
				return "", err
			}
			rpt, err = s.RequestRPT(accessToken, oldRPT)
			if err != nil {
				return "", err
			}
		} else {
			return
		}
	}
	cs.SetRPT(repoURI, rpt)
	if err = cs.Flush(); err != nil {
		return
	}
	cmd.Printf("Saved credentials to %s\n", cs.Path())
	return
}
