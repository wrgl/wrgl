// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

import (
	"net/http"

	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/ref"
)

const PathRefs = "/refs/"

type GetRefsHandler struct {
	rs ref.Store
}

func NewGetRefsHandler(rs ref.Store) *GetRefsHandler {
	return &GetRefsHandler{rs: rs}
}

func (h *GetRefsHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	refs, err := ref.ListLocalRefs(h.rs)
	if err != nil {
		panic(err)
	}
	resp := &payload.GetRefsResponse{
		Refs: map[string]*payload.Hex{},
	}
	for k, v := range refs {
		h := &payload.Hex{}
		copy((*h)[:], v)
		resp.Refs[k] = h
	}
	writeJSON(rw, resp)
}
