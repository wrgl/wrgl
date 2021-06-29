// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"fmt"
	"strings"
)

type Refspec struct {
	Force      bool
	Negate     bool
	src        string
	srcStarInd int
	dst        string
	dstStarInd int
	tag        string
}

func NewRefspec(src, dst string, negate, force bool) (rs *Refspec, err error) {
	rs = &Refspec{
		Force:  force,
		Negate: negate,
		src:    src,
		dst:    dst,
	}
	rs.srcStarInd, err = isGlobPattern(rs.src)
	if err != nil {
		return nil, err
	}
	rs.dstStarInd, err = isGlobPattern(rs.dst)
	if err != nil {
		return nil, err
	}
	return
}

func (s *Refspec) IsGlob() bool {
	return s.srcStarInd != -1
}

func (s *Refspec) Src() string {
	if s.tag != "" {
		return "refs/tags/" + s.tag
	}
	return s.src
}

func (s *Refspec) Dst() string {
	if s.tag != "" {
		return "refs/tags/" + s.tag
	}
	return s.dst
}

func (s *Refspec) SrcMatchRef(r string) bool {
	src := s.Src()
	if s.srcStarInd == -1 {
		return src == r
	}
	if s.srcStarInd >= len(r) {
		return false
	}
	return r[:s.srcStarInd] == src[:s.srcStarInd]
}

func (s *Refspec) Exclude(r string) bool {
	return s.Negate && s.SrcMatchRef(r)
}

func (s *Refspec) DstMatchRef(r string) bool {
	dst := s.Dst()
	if dst == "" || r == "" {
		return false
	}
	if s.dstStarInd == -1 {
		return dst == r
	}
	if s.dstStarInd >= len(r) {
		return false
	}
	return r[:s.dstStarInd] == dst[:s.dstStarInd]
}

func (s *Refspec) DstForRef(p string) string {
	dst := s.Dst()
	if dst == "" || p == "" {
		return ""
	}
	src := s.Src()
	if s.srcStarInd == -1 {
		if src == p {
			return dst
		}
		return ""
	} else if p[:s.srcStarInd] != src[:s.srcStarInd] {
		return ""
	}
	return dst[:s.dstStarInd] + p[s.srcStarInd:]
}

func (s *Refspec) String() string {
	sl := []string{}
	if s.Force {
		sl = append(sl, "+")
	}
	if s.Negate {
		sl = append(sl, "^")
	}
	if s.tag != "" {
		sl = append(sl, "tag ", s.tag)
	} else {
		sl = append(sl, s.src)
		if s.dst != "" {
			sl = append(sl, ":", s.dst)
		}
	}
	return strings.Join(sl, "")
}

func (s *Refspec) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func isGlobPattern(s string) (int, error) {
	i := strings.IndexRune(s, '*')
	if i == -1 {
		return -1, nil
	}
	if i != len(s)-1 {
		return 0, fmt.Errorf("invalid glob pattern %q: there can only be one '*' at the end", s)
	}
	return i, nil
}

func (s *Refspec) UnmarshalText(text []byte) error {
	off := 0
	n := len(text)
	if text[0] == '+' {
		s.Force = true
		off += 1
	}
	if text[0] == '^' {
		s.Negate = true
		off += 1
	}
	if string(text[off:off+4]) == "tag " {
		s.tag = string(text[4:])
		return nil
	}
	i := 0
	for i = off; i < n; i++ {
		if text[i] == ':' {
			break
		}
	}
	s.src = string(text[off:i])
	if i < n {
		s.dst = string(text[i+1:])
	}
	var err error
	s.srcStarInd, err = isGlobPattern(s.src)
	if err != nil {
		return err
	}
	s.dstStarInd, err = isGlobPattern(s.dst)
	if err != nil {
		return err
	}
	if s.Negate {
		if s.dst != "" {
			return fmt.Errorf("must not specify dst in negated refspec")
		}
	} else if (s.srcStarInd != -1 && s.dstStarInd == -1) || (s.dstStarInd != -1 && s.srcStarInd == -1) {
		return fmt.Errorf("both src and dst must be glob patterns if one is a glob pattern")
	}
	return nil
}

func ParseRefspec(s string) (*Refspec, error) {
	rs := &Refspec{}
	err := rs.UnmarshalText([]byte(s))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func MustParseRefspec(s string) *Refspec {
	rs := &Refspec{}
	err := rs.UnmarshalText([]byte(s))
	if err != nil {
		panic(err.Error())
	}
	return rs
}
