package auth

import "github.com/golang-jwt/jwt"

type Claims struct {
	jwt.StandardClaims
	Email  string   `json:"email,omitempty"`
	Name   string   `json:"name,omitempty"`
	Scopes []string `json:"scopes,omitempty"`
}
