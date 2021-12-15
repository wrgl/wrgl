// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

type Remote struct {
	// URL is the URL of a remote.
	URL string

	// Fetch is the list of refspecs to fetch from this remote when
	// user run `wrgl fetch <remote>` without specifying refspecs.
	Fetch RefspecSlice `yaml:"fetch,omitempty" json:"fetch,omitempty"`

	// Push is the list of refspecs to push to this remote when user
	// run `wrgl push <remote>` without specifying refspecs.
	Push RefspecSlice `yaml:"push,omitempty" json:"push,omitempty"`

	// Mirror, when set to `true`, `wrgl push <remote>` behaves as if
	// flag `--mirror` is set. This remote will then act as a mirror
	// of the local repository.
	Mirror bool `yaml:"mirror,omitempty" json:"mirror,omitempty"`
}

func (cr *Remote) FetchDstForRef(r string) string {
	s := ""
	for _, rs := range cr.Fetch {
		if rs.Exclude(r) {
			s = ""
			break
		}
		if s == "" {
			s = rs.DstForRef(r)
		}
	}
	return s
}

func (cr *Remote) FetchDstMatchRef(r string) bool {
	for _, rs := range cr.Fetch {
		if rs.DstMatchRef(r) {
			return true
		}
	}
	return false
}
