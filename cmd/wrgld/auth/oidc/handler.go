package authoidc

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/coreos/go-oidc"
	"github.com/gobwas/glob"
	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	"github.com/wrgl/wrgl/pkg/api"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/conf"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

type Client struct {
	RedirectURIs []glob.Glob
}

type Handler struct {
	clients     map[string]Client
	corsOrigins []string
	provider    *oidc.Provider
	verifier    *oidc.IDTokenVerifier
	oidcConfig  *oauth2.Config
	handler     http.Handler

	sessions *SessionManager
}

func NewHandler(serverHandler http.Handler, c *conf.Config, client *http.Client) (h *Handler, err error) {
	if c == nil || c.Auth == nil || c.Auth.OAuth2 == nil {
		return nil, fmt.Errorf("empty auth.oauth2 config")
	}
	if c.Auth.OAuth2.OIDCProvider == nil {
		return nil, fmt.Errorf("empty auth.oauth2.oidcProvider config")
	}
	if len(c.Auth.OAuth2.Clients) == 0 {
		return nil, fmt.Errorf("no registered client (empty auth.oauth2.clients config)")
	}
	h = &Handler{
		clients:  map[string]Client{},
		sessions: NewSessionManager(),
	}
	for _, c := range c.Auth.OAuth2.Clients {
		client := &Client{}
		if len(c.RedirectURIs) == 0 {
			return nil, fmt.Errorf("empty redirectURIs for client %q", c.ID)
		}
		for _, s := range c.RedirectURIs {
			u, err := url.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("error parsing url %q", s)
			}
			h.corsOrigins = append(h.corsOrigins, fmt.Sprintf("%s://%s", u.Scheme, u.Host))
			g, err := glob.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("error compiling glob pattern %q", s)
			}
			client.RedirectURIs = append(client.RedirectURIs, g)
		}
		h.clients[c.ID] = *client
		log.Printf("client %q registered", c.ID)
	}
	ctx := context.Background()
	if client != nil {
		ctx = oidc.ClientContext(ctx, client)
	}
	if err = backoff.RetryNotify(
		func() (err error) {
			h.provider, err = oidc.NewProvider(ctx, c.Auth.OAuth2.OIDCProvider.Issuer)
			return err
		},
		backoff.NewExponentialBackOff(),
		func(e error, d time.Duration) {
			log.Printf("error creating oidc provider: %v. backoff for %s", e, d)
		},
	); err != nil {
		return nil, err
	}
	h.oidcConfig = &oauth2.Config{
		ClientID:     c.Auth.OAuth2.OIDCProvider.ClientID,
		ClientSecret: c.Auth.OAuth2.OIDCProvider.ClientSecret,
		RedirectURL:  strings.TrimRight(c.Auth.OAuth2.OIDCProvider.Address, "/") + "/oidc/callback/",
		Endpoint:     h.provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}
	h.verifier = h.provider.Verifier(&oidc.Config{
		ClientID: c.Auth.OAuth2.OIDCProvider.ClientID,
	})

	sm := http.NewServeMux()
	sm.HandleFunc("/oauth2/authorize/", h.handleAuthorize)
	sm.HandleFunc("/oauth2/token/", h.handleToken)
	sm.HandleFunc("/oauth2/devicecode/", h.handleDeviceCode)
	sm.HandleFunc("/oauth2/device/", h.handleDevice)
	sm.HandleFunc("/oidc/callback/", h.handleCallback)
	sm.Handle("/", wrgldutils.ApplyMiddlewares(
		serverHandler,
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				c := getClaims(r)
				if c != nil {
					r = apiserver.SetEmail(apiserver.SetName(r, c.Name), c.Email)
				}
				h.ServeHTTP(rw, r)
			})
		},
		apiserver.AuthorizeMiddleware(apiserver.AuthzMiddlewareOptions{
			Enforce: func(r *http.Request, scope string) bool {
				c := getClaims(r)
				if c != nil {
					for _, s := range c.Roles {
						if s == scope {
							return true
						}
					}
				}
				return false
			},
			GetConfig: func(r *http.Request) *conf.Config {
				return c
			},
		}),
		h.validateAccessToken,
	))
	h.handler = wrgldutils.ApplyMiddlewares(
		sm,
		h.CORSMiddleware,
	)

	return h, nil
}

func (h *Handler) CORSMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		for _, s := range h.corsOrigins {
			rw.Header().Add("Access-Control-Allow-Origin", s)
		}
		if r.Method == http.MethodOptions {
			rw.Header().Set("Access-Control-Allow-Methods", strings.Join([]string{
				http.MethodOptions, http.MethodGet, http.MethodPost, http.MethodPut,
			}, ", "))
			rw.Header().Set("Access-Control-Allow-Headers", strings.Join([]string{
				"Authorization",
				"Cache-Control",
				"Pragma",
				"Content-Encoding",
				"Trailer",
				api.HeaderPurgeUploadPackSession,
			}, ", "))
		} else {
			handler.ServeHTTP(rw, r)
		}
	})
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
	log.Printf("validating client id %q", clientID)
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

func (h *Handler) cloneOauth2Config() *oauth2.Config {
	c := &oauth2.Config{}
	*c = *h.oidcConfig
	return c
}

func (h *Handler) parseForm(r *http.Request) (url.Values, error) {
	if r.Method == http.MethodGet {
		return r.URL.Query(), nil
	}
	if r.Method == http.MethodPost {
		if s := r.Header.Get("Content-Type"); !strings.Contains(s, "application/x-www-form-urlencoded") {
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

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(rw, r)
}

func writeHTML(rw http.ResponseWriter, tmpl *template.Template, data interface{}) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(rw, data); err != nil {
		panic(err)
	}
}
