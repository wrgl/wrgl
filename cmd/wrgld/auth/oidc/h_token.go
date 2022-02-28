package authoidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	apiserver "github.com/wrgl/wrgl/pkg/api/server"
)

func (h *Handler) getSessionForTokenEndpoint(values url.Values, stateKey string) (state string, session *Session, err error) {
	state = values.Get(stateKey)
	if state == "" {
		err = &Oauth2Error{"invalid_request", stateKey + " required"}
		return
	}
	session = h.sessions.PopWithState(state)
	if session == nil {
		err = &Oauth2Error{"invalid_request", "invalid " + stateKey}
		return
	}
	if s := values.Get("client_id"); s != session.ClientID {
		err = &Oauth2Error{"invalid_client", "invalid client_id"}
		return
	}
	return state, session, nil
}

func (h *Handler) handleToken(rw http.ResponseWriter, r *http.Request) {
	values, err := h.parseForm(r)
	if err != nil {
		handleError(rw, err)
		return
	}
	var code string
	grantType := values.Get("grant_type")
	switch grantType {
	case "authorization_code":
		state, session, err := h.getSessionForTokenEndpoint(values, "code")
		if err != nil {
			handleError(rw, err)
			return
		}
		redirectURI, err := url.Parse(values.Get("redirect_uri"))
		if err != nil {
			handleError(rw, &Oauth2Error{"invalid_request", "failed to parse redirect_uri"})
			return
		}
		if s := fmt.Sprintf("%s://%s%s", redirectURI.Scheme, redirectURI.Host, redirectURI.Path); s != session.RedirectURI {
			log.Printf("redirect URI does not match %q != %q", s, session.RedirectURI)
			handleError(rw, &Oauth2Error{"invalid_request", "redirect_uri does not match"})
			return
		}
		if s := values.Get("code_verifier"); s == "" {
			handleError(rw, &Oauth2Error{"invalid_grant", "code_verifier required"})
			return
		} else {
			hash := sha256.New()
			if _, err := hash.Write([]byte(s)); err != nil {
				panic(err)
			}
			if base64.RawURLEncoding.EncodeToString(hash.Sum([]byte{})) != session.CodeChallenge {
				handleError(rw, &Oauth2Error{"invalid_grant", "code challenge failed"})
				return
			}
		}
		code = state
	case "urn:ietf:params:oauth:grant-type:device_code":
		_, session, err := h.getSessionForTokenEndpoint(values, "device_code")
		if err != nil {
			handleError(rw, err)
			return
		}
		code = session.Code
	default:
		handleError(rw, &Oauth2Error{"invalid_request", "invalid grant_type"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	token, err := h.oidcConfig.Exchange(ctx, code)
	if err != nil {
		log.Printf("error: error exchanging code: %v", err)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	if token.TokenType != "Bearer" {
		log.Printf("error: expected bearer token, found %q", token.TokenType)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		log.Printf("error: no id_token field in oauth2 token")
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	if _, err = h.verifier.Verify(context.Background(), rawIDToken); err != nil {
		log.Printf("error: failed to verify id_token: %v", err)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}

	rw.Header().Set("Cache-Control", "no-store")
	rw.Header().Set("Pragma", "no-cache")
	apiserver.WriteJSON(rw, &TokenResponse{
		AccessToken: rawIDToken,
		TokenType:   token.TokenType,
	})
}
