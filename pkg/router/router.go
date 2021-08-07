// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package router

import (
	"net/http"
	"net/url"
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
	redirect := !strings.HasSuffix(path, "/")
	if redirect {
		path = path + "/"
	}
	if routes.Pat != nil {
		if m := routes.Pat.FindStringSubmatch(path); m != nil {
			path = strings.TrimPrefix(path, m[0])
		} else {
			http.NotFound(rw, r)
			return
		}
	}
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
	if redirect {
		u := &url.URL{}
		*u = *r.URL
		u.Path = r.URL.Path + "/"
		http.Redirect(rw, r, u.String(), http.StatusMovedPermanently)
		return
	}
	routes.Handler.ServeHTTP(rw, r)
}
