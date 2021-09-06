// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package authfs

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/wrgl/core/pkg/auth"
)

func createIDToken(email string, jwtSecret []byte, duration time.Duration) (string, error) {
	claims := &auth.Claims{
		StandardClaims: jwt.StandardClaims{
			Issuer:    "Wrgld",
			ExpiresAt: time.Now().Add(duration).Unix(),
			IssuedAt:  time.Now().Unix(),
		},
		Email: email,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}

func validateIDToken(tokenString string, jwtSecret []byte) (claims *auth.Claims, err error) {
	token, err := jwt.ParseWithClaims(tokenString, &auth.Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})
	if err != nil {
		if strings.HasPrefix(err.Error(), "token is expired by") && token != nil {
			claims := token.Claims.(*auth.Claims)
			fmt.Printf("expires at: %v\n", claims.ExpiresAt)
			fmt.Printf("now: %v\n", time.Now())
		}
		return
	}
	if claims, ok := token.Claims.(*auth.Claims); ok && token.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}
