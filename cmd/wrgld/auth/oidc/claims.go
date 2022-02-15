package authoidc

import (
	"context"
	"net/http"
)

type ResourceAccess struct {
	Roles []string `json:"roles"`
}

type Claims struct {
	Email          string                    `json:"email"`
	Name           string                    `json:"name"`
	ResourceAccess map[string]ResourceAccess `json:"resource_access"`
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
