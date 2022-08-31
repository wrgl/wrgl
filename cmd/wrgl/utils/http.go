// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"context"
	"net/http"
)

type httpClientKey struct{}

func SetClient(ctx context.Context, client *http.Client) context.Context {
	return context.WithValue(ctx, httpClientKey{}, client)
}

func GetClient(ctx context.Context) *http.Client {
	if i := ctx.Value(httpClientKey{}); i != nil {
		return i.(*http.Client)
	}
	return http.DefaultClient
}
