// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"net/http"
	"regexp"
	"strings"
)

type Routes struct {
	Method  string
	Pat     *regexp.Regexp
	Handler http.Handler
	Subs    []*Routes
}

type Router struct {
	c *Routes
}

func NewRouter(c *Routes) *Router {
	return &Router{
		c: c,
	}
}

func (router *Router) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	routes := router.c
	path := r.URL.Path
mainLoop:
	for {
		var defaultRoutes *Routes
		for _, obj := range routes.Subs {
			if obj.Method != "" && obj.Method != r.Method {
				continue
			}
			if obj.Pat == nil {
				defaultRoutes = obj
			} else {
				if m := obj.Pat.FindStringSubmatch(path); m != nil {
					path = strings.TrimPrefix(path, m[0])
					routes = obj
					continue mainLoop
				}
			}
		}
		if defaultRoutes != nil {
			routes = defaultRoutes
			continue
		}
		break
	}
	if routes.Handler == nil {
		http.NotFound(rw, r)
		return
	}
	routes.Handler.ServeHTTP(rw, r)
}
