// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

type UploadPackRequest struct {
	Wants []*Hex `json:"wants,omitempty"`
	Haves []*Hex `json:"haves,omitempty"`
	Done  bool   `json:"done,omitempty"`
}

type UploadPackResponse struct {
	ACKs []*Hex `json:"acks,omitempty"`
}
