// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package payload

// UploadPackRequest is a request to the /upload-pack/ endpoint
type UploadPackRequest struct {
	// Wants is the list of wanted commits. Unless Depth is also specified, all
	// ancestors of Wants that are not present on the client will also be sent.
	// In an /upload-pack/ session, Wants must not be empty in the first request
	// and be empty in subsequent requests.
	Wants []*Hex `json:"wants,omitempty"`

	// Haves is the list of non-partial commits present on the client. The server
	// look at this list to figure out the minimal set of commits to send. By
	// default each request has a maximum of 32 haves.
	Haves []*Hex `json:"haves,omitempty"`

	// Depth is the maximum depth pass which the commits will only be transfered
	// shallowly rather than in full. A shallow commit transfer excludes its table
	// so while the commit history is still available, you won't be able to access
	// their data until you have pulled such commits in full. A Depth of 0 means
	// all missing commits will be transfered in full
	Depth int `json:"depth,omitempty"`

	// Done when set to true, signifies that the client is done with negotiation
	// and would like to receive all the commits that the server thinks are missing
	// on the client side. Done is typically set to true when the client has already
	// exhausted its history or has sent 256 haves and still not done with negotiation
	Done bool `json:"done,omitempty"`

	// TableACKs is the list of tables present on both the client and the server. The
	// server will not send tables in this list.
	TableACKs []*Hex `json:"tableACKs,omitempty"`
}

// UploadPackResponse is a possible response from the /upload-pack/ endpoint.
// It's primary purpose is to let the client know which commits the server acknowledge
// is present and to nudge the client to continue negotiation. When negotiation is
// finished, the server will send a Packfile with all the missing objects instead.
type UploadPackResponse struct {
	// ACKs is the list of commits that are present (but not necessarily in full) on the
	// server. The client can safely remove these commits and their ancestors from
	// negotiation.
	ACKs []*Hex `json:"acks,omitempty"`

	// TableHaves is the list of tables present on the server. The client put one entry
	// for each table that it haves in tableACKs in the next request
	TableHaves []*Hex `json:"tableHaves,omitempty"`
}
