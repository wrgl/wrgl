// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

type Remote struct {
	URL    string
	Fetch  []*Refspec `yaml:"fetch,omitempty" json:"fetch,omitempty"`
	Push   []*Refspec `yaml:"push,omitempty" json:"push,omitempty"`
	Mirror bool       `yaml:"mirror,omitempty" json:"mirror,omitempty"`
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
