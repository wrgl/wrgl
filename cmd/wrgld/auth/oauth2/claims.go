package authoauth2

import (
	"context"
	"net/http"

	"github.com/coreos/go-oidc"
)

type Claims struct {
	oidc.IDToken

	Email string   `json:"email"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

func (c *Claims) Valid() error {
	return nil
}

type claimsKey struct{}

func setClaims(r *http.Request, claims *Claims) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims))
}

func getClaims(r *http.Request) *Claims {
	if i := r.Context().Value(claimsKey{}); i != nil {
		return i.(*Claims)
	}
	return nil
}
