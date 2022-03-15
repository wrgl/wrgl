package authoauth2

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/coreos/go-oidc"
	"github.com/wrgl/wrgl/pkg/conf"
	"golang.org/x/oauth2"
)

type OIDCProvider interface {
	Verify(ctx context.Context, rawIDToken string) error
	Claims(ctx context.Context, rawIDToken string) (*Claims, error)
	Exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error)
	AuthCodeURL(state string, opts ...oauth2.AuthCodeOption) string
}

type oidcProvider struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	config   *oauth2.Config
}

func NewOIDCProvider(c *conf.AuthOIDCProvider, client *http.Client) (OIDCProvider, error) {
	ctx := context.Background()
	if client != nil {
		ctx = oidc.ClientContext(ctx, client)
	}
	p := &oidcProvider{}
	if err := backoff.RetryNotify(
		func() (err error) {
			p.provider, err = oidc.NewProvider(ctx, c.Issuer)
			return err
		},
		backoff.NewExponentialBackOff(),
		func(e error, d time.Duration) {
			log.Printf("error creating oidc provider: %v. backoff for %s", e, d)
		},
	); err != nil {
		return nil, err
	}
	p.config = &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		RedirectURL:  strings.TrimRight(c.Address, "/") + "/oidc/callback/",
		Endpoint:     p.provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}
	p.verifier = p.provider.Verifier(&oidc.Config{
		ClientID: c.ClientID,
	})
	return p, nil
}

func (p *oidcProvider) Verify(ctx context.Context, rawIDToken string) error {
	_, err := p.verifier.Verify(ctx, rawIDToken)
	return err
}

func (p *oidcProvider) Claims(ctx context.Context, rawIDToken string) (*Claims, error) {
	token, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, err
	}
	c := &Claims{}
	if err = token.Claims(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *oidcProvider) Exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	return p.config.Exchange(ctx, code, opts...)
}

func (p *oidcProvider) cloneOauth2Config() *oauth2.Config {
	c := &oauth2.Config{}
	*c = *p.config
	return c
}

func (p *oidcProvider) AuthCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	c := p.cloneOauth2Config()
	return c.AuthCodeURL(state, opts...)
}
