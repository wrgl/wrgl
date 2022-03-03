package authoauth2

import (
	"context"
	"net/http"
)

type Claims struct {
	Email string   `json:"email"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
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
