package authoauth2

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gobwas/glob"
	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	"github.com/wrgl/wrgl/pkg/api"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/conf"
	"golang.org/x/net/context"
)

type Client struct {
	RedirectURIs []glob.Glob
}

type Handler struct {
	clients     map[string]Client
	corsOrigins []string
	provider    OIDCProvider
	handler     http.Handler
	address     string

	sessions *SessionManager
}

func NewHandler(serverHandler http.Handler, config *conf.Config, provider OIDCProvider) (h *Handler, err error) {
	if len(config.Auth.OAuth2.Clients) == 0 {
		return nil, fmt.Errorf("no registered client (empty auth.oauth2.clients config)")
	}
	h = &Handler{
		clients:  map[string]Client{},
		sessions: NewSessionManager(),
		provider: provider,
		address:  config.Auth.OAuth2.OIDCProvider.Address,
	}
	for _, c := range config.Auth.OAuth2.Clients {
		client := &Client{}
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
				return config
			},
		}),
		h.validateAccessToken,
	))
	sm.Handle("/static/", http.FileServer(http.FS(contentFS)))
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
			c, err := h.provider.Claims(ctx, rawIDToken)
			if err != nil {
				log.Printf("failed to verify access_token: %v", err)
				apiserver.SendError(rw, http.StatusUnauthorized, "unauthorized")
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

func (h *Handler) parsePOSTForm(r *http.Request) (url.Values, error) {
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

func (h *Handler) parseForm(r *http.Request) (url.Values, error) {
	if r.Method == http.MethodGet {
		return r.URL.Query(), nil
	}
	return h.parsePOSTForm(r)
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
