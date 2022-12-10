package utils

import (
	"github.com/golang-jwt/jwt/v4"
)

func jwtIsValid(token string) bool {
	p := jwt.NewParser()
	claims := &jwt.MapClaims{}
	_, _, err := p.ParseUnverified(token, claims)
	if err != nil {
		return false
	}
	return claims.Valid() != nil
}
