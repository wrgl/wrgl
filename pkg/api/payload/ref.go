// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

type GetRefsResponse struct {
	Refs map[string]*Hex `json:"refs"`
}
