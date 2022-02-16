package authoidc

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/gobwas/glob"
	"github.com/google/uuid"
	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/conf"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

var validScopes map[string]struct{}

func init() {
	validScopes = map[string]struct{}{
		auth.ScopeRepoRead:        {},
		auth.ScopeRepoWrite:       {},
		auth.ScopeRepoReadConfig:  {},
		auth.ScopeRepoWriteConfig: {},
	}
}

type ClientSession struct {
	ClientID            string
	RedirectURI         string
	State               string
	CodeChallenge       string
	CodeChallengeMethod string
}

type Client struct {
	RedirectURIs []glob.Glob
}

type Handler struct {
	clients    map[string]Client
	provider   *oidc.Provider
	verifier   *oidc.IDTokenVerifier
	oidcConfig *oauth2.Config
	sm         *http.ServeMux

	stateMap    map[string]*ClientSession
	stateMutext sync.Mutex
}

func NewHandler(serverHandler http.Handler, authConf *conf.Auth, client *http.Client) (h *Handler, err error) {
	if authConf == nil {
		return nil, fmt.Errorf("empty auth config")
	}
	if authConf.OidcProvider == nil {
		return nil, fmt.Errorf("empty auth.oidcProvider config")
	}
	if len(authConf.Clients) == 0 {
		return nil, fmt.Errorf("no registered client (empty auth.clients config)")
	}
	h = &Handler{
		sm:       http.NewServeMux(),
		stateMap: map[string]*ClientSession{},
		clients:  map[string]Client{},
	}
	for _, c := range authConf.Clients {
		client := &Client{}
		if len(c.RedirectURIs) == 0 {
			return nil, fmt.Errorf("empty redirectURIs for client %q", c.ID)
		}
		for _, s := range c.RedirectURIs {
			g, err := glob.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("error compiling glob pattern %q", s)
			}
			client.RedirectURIs = append(client.RedirectURIs, g)
		}
		h.clients[c.ID] = *client
	}
	ctx := context.Background()
	if client != nil {
		ctx = oidc.ClientContext(ctx, client)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	h.provider, err = oidc.NewProvider(ctx, authConf.OidcProvider.Issuer)
	if err != nil {
		return nil, err
	}
	h.oidcConfig = &oauth2.Config{
		ClientID:     authConf.OidcProvider.ClientID,
		ClientSecret: authConf.OidcProvider.ClientSecret,
		RedirectURL:  strings.TrimRight(authConf.OidcProvider.Address, "/") + "/oidc/callback/",
		Endpoint:     h.provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}
	h.verifier = h.provider.Verifier(&oidc.Config{
		ClientID: authConf.OidcProvider.ClientID,
	})

	h.sm.HandleFunc("/oauth2/authorize/", h.handleAuthorize)
	h.sm.HandleFunc("/oauth2/token/", h.handleToken)
	h.sm.HandleFunc("/oidc/callback/", h.handleCallback)
	h.sm.Handle("/", wrgldutils.ApplyMiddlewares(
		serverHandler,
		apiserver.AuthorizeMiddleware(apiserver.AuthzMiddlewareOptions{
			GetEmailName: func(r *http.Request) (email string, name string) {
				c := getClaims(r)
				if c != nil {
					email = c.Email
					name = c.Name
				}
				return
			},
			GetScopes: func(r *http.Request) (scopes []string) {
				c := getClaims(r)
				if c != nil {
					if ra, ok := c.ResourceAccess[authConf.OidcProvider.ClientID]; ok {
						scopes = ra.Roles
					}
				}
				return
			},
			RequestScope: func(rw http.ResponseWriter, r *http.Request, scope string) {
				var scopes []string
				c := getClaims(r)
				if c != nil {
					if ra, ok := c.ResourceAccess[authConf.OidcProvider.ClientID]; ok {
						scopes = ra.Roles
					}
				}
				found := false
				for _, s := range scopes {
					if s == scope {
						found = true
						break
					}
				}
				if !found {
					scopes = append(scopes, scope)
				}
				handleError(rw,
					&UnauthorizedError{
						Message:      fmt.Sprintf("scope %q required", scope),
						CurrentScope: strings.Join(scopes, " "),
						MissingScope: scope,
					})
			},
		}),
		h.validateAccessToken,
	))

	return h, nil
}

func (h *Handler) validateAccessToken(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if s := r.Header.Get("Authorization"); s != "" {
			rawIDToken := strings.Split(s, " ")[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()
			token, err := h.verifier.Verify(ctx, rawIDToken)
			if err != nil {
				log.Printf("failed to verify access_token: %v", err)
				apiserver.SendError(rw, http.StatusUnauthorized, "unauthorized")
				return
			}
			c := &Claims{}
			if err = token.Claims(c); err != nil {
				log.Printf("error parsing claims: %v", err)
				apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
				return
			}
			r = setClaims(r, c)
		}
		handler.ServeHTTP(rw, r)
	})
}

func (h *Handler) validClientID(clientID string) bool {
	for id := range h.clients {
		if clientID == id {
			return true
		}
	}
	return false
}

func (h *Handler) validRedirectURI(clientID, uri string) bool {
	if c, ok := h.clients[clientID]; ok {
		for _, r := range c.RedirectURIs {
			if r.Match(uri) {
				return true
			}
		}
	}
	return false
}

func (h *Handler) configWithScopes(scopes ...string) *oauth2.Config {
	c := &oauth2.Config{}
	*c = *h.oidcConfig
	c.Scopes = append(c.Scopes, scopes...)
	return c
}

func (h *Handler) saveSession(state string, ses *ClientSession) string {
	h.stateMutext.Lock()
	defer h.stateMutext.Unlock()
	if state == "" {
		state = uuid.New().String()
	}
	h.stateMap[state] = ses
	return state
}

func (h *Handler) getSession(state string) *ClientSession {
	h.stateMutext.Lock()
	defer h.stateMutext.Unlock()
	if v, ok := h.stateMap[state]; ok {
		delete(h.stateMap, state)
		return v
	}
	return nil
}

func (h *Handler) parseForm(r *http.Request) (url.Values, error) {
	if r.Method == http.MethodGet {
		return r.URL.Query(), nil
	}
	if r.Method == http.MethodPost {
		if s := r.Header.Get("Content-Type"); s != "application/x-www-form-urlencoded" {
			return nil, &HTTPError{http.StatusBadRequest, fmt.Sprintf("unsupported content type %q", s)}
		}
		defer r.Body.Close()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		return url.ParseQuery(string(b))
	}
	return nil, &HTTPError{http.StatusMethodNotAllowed, "method not allowed"}
}

func (h *Handler) handleAuthorize(rw http.ResponseWriter, r *http.Request) {
	values, err := h.parseForm(r)
	if err != nil {
		handleError(rw, err)
		return
	}
	if s := values.Get("response_type"); s != "code" {
		handleError(rw, &Oauth2Error{"invalid_request", "response_type must be code"})
		return
	}
	clientID := values.Get("client_id")
	if h.validClientID(clientID) {
		handleError(rw, &Oauth2Error{"invalid_client", "unknown cient"})
		return
	}
	var scopes []string
	if s := values.Get("scope"); s != "" {
		scopes = strings.Split(s, " ")
		for _, s := range scopes {
			if _, ok := validScopes[s]; !ok {
				handleError(rw, &Oauth2Error{"invalid_scope", fmt.Sprintf("unknown scope %q", s)})
				return
			}
		}
	} else {
		scopes = []string{auth.ScopeRepoRead}
	}
	for _, key := range []string{
		"code_challenge",
		"state",
		"redirect_uri",
	} {
		if s := values.Get(key); s != "" {
			handleError(rw, &Oauth2Error{"invalid_request", fmt.Sprintf("%s required", key)})
			return
		}
	}
	if s := values.Get("code_challenge_method"); s != "S256" {
		handleError(rw, &Oauth2Error{"invalid_request", "code_challenge_method must be S256"})
		return
	}
	redirectURI := values.Get("redirect_uri")
	if !h.validRedirectURI(clientID, redirectURI) {
		handleError(rw, &Oauth2Error{"invalid_request", "invalid redirect_uri"})
		return
	}
	if _, err := url.Parse(redirectURI); err != nil {
		handleError(rw, &Oauth2Error{"invalid_request", fmt.Sprintf("invalid redirect_uri: %v", err)})
		return
	}
	state := h.saveSession("", &ClientSession{
		ClientID:            clientID,
		State:               values.Get("state"),
		RedirectURI:         redirectURI,
		CodeChallenge:       values.Get("code_challenge"),
		CodeChallengeMethod: values.Get("code_challenge_method"),
	})
	oauth2Config := h.configWithScopes(scopes...)
	http.Redirect(rw, r, oauth2Config.AuthCodeURL(state), http.StatusFound)
}

func (h *Handler) handleCallback(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	state := values.Get("state")
	if state == "" {
		handleError(rw, &Oauth2Error{"invalid_request", "state is missing"})
		return
	}
	session := h.getSession(state)
	if session == nil {
		handleError(rw, &Oauth2Error{"invalid_request", "invalid state"})
		return
	}
	code := values.Get("code")
	h.saveSession(code, &ClientSession{
		RedirectURI:         session.RedirectURI,
		ClientID:            session.ClientID,
		CodeChallenge:       session.CodeChallenge,
		CodeChallengeMethod: session.CodeChallengeMethod,
	})
	uri, err := url.Parse(session.RedirectURI)
	if err != nil {
		log.Printf("error parsing redirect_uri: %v", err)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	query := uri.Query()
	query.Set("state", session.State)
	query.Set("code", code)
	uri.RawQuery = query.Encode()
	http.Redirect(rw, r, uri.String(), http.StatusFound)
}

func (h *Handler) handleToken(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	if s := values.Get("grant_type"); s != "authorization_code" {
		handleError(rw, &Oauth2Error{"invalid_request", "grant_type must be authorization_code"})
		return
	}
	code := values.Get("code")
	if code == "" {
		handleError(rw, &Oauth2Error{"invalid_request", "code required"})
		return
	}
	session := h.getSession(values.Get("code"))
	if session == nil {
		handleError(rw, &Oauth2Error{"invalid_request", "invalid code"})
		return
	}
	if s := values.Get("client_id"); s != session.ClientID {
		handleError(rw, &Oauth2Error{"invalid_client", "invalid client_id"})
		return
	}
	if s := values.Get("redirect_uri"); s != session.RedirectURI {
		handleError(rw, &Oauth2Error{"invalid_request", "redirect_uri does not match"})
		return
	}
	if s := values.Get("code_verifier"); s == "" {
		handleError(rw, &Oauth2Error{"invalid_grant", "code_verifier required"})
		return
	} else {
		sum := sha256.Sum256([]byte(strconv.QuoteToASCII(s)))
		if base64.URLEncoding.EncodeToString(sum[:]) != session.CodeChallenge {
			handleError(rw, &Oauth2Error{"invalid_grant", "code challenge failed"})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	token, err := h.oidcConfig.Exchange(ctx, values.Get("code"))
	if err != nil {
		log.Printf("error exchanging code: %v", err)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	if token.TokenType != "Bearer" {
		log.Printf("expected bearer token, found %q", token.TokenType)
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		log.Printf("no id_token field in oauth2 token")
		apiserver.SendError(rw, http.StatusInternalServerError, "internal server error")
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	if _, err = h.verifier.Verify(ctx, rawIDToken); err != nil {
		log.Printf("failed to verify id_token: %v", err)
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

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h.sm.ServeHTTP(rw, r)
}
