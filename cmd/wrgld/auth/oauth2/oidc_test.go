package authoauth2

import (
	"context"
	"fmt"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/testutils"
	"golang.org/x/oauth2"
)

type mockOIDCProvider struct {
	URL    string
	tokens map[string]*Claims
	secret []byte
}

func newMockOIDCProvider() *mockOIDCProvider {
	return &mockOIDCProvider{
		URL:    "http://oidc.provider",
		tokens: map[string]*Claims{},
		secret: testutils.SecureRandomBytes(20),
	}
}

func (p *mockOIDCProvider) Verify(ctx context.Context, rawIDToken string) error {
	_, err := jwt.ParseWithClaims(rawIDToken, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return p.secret, nil
	})
	return err
}

func (p *mockOIDCProvider) Claims(ctx context.Context, rawIDToken string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(rawIDToken, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return p.secret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}

func (p *mockOIDCProvider) PrepareCode(claims *Claims) (code string) {
	code = uuid.New().String()
	p.tokens[code] = claims
	return
}

func (p *mockOIDCProvider) Exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	if claims, ok := p.tokens[code]; ok {
		token := &oauth2.Token{
			TokenType: "Bearer",
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		idToken, err := tok.SignedString(p.secret)
		if err != nil {
			return nil, err
		}
		return token.WithExtra(map[string]interface{}{
			"id_token": idToken,
		}), nil
	}
	return nil, fmt.Errorf("code not found")
}

func (p *mockOIDCProvider) AuthCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	return fmt.Sprintf("%s/auth/?state=%s", p.URL, state)
}
