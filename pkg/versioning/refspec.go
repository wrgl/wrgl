package versioning

import (
	"fmt"
	"strings"
)

type Refspec struct {
	Plus   bool
	Negate bool
	src    string
	dst    string
	tag    string
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

func (s *Refspec) String() string {
	sl := []string{}
	if s.Plus {
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

func (s *Refspec) UnmarshalText(text []byte) error {
	off := 0
	n := len(text)
	if text[0] == '+' {
		s.Plus = true
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
	srcIsPat := strings.ContainsRune(s.src, '*')
	dstIsPat := strings.ContainsRune(s.dst, '*')
	if s.Negate {
		if s.dst != "" {
			return fmt.Errorf("must not specify dst in negated refspec")
		}
	} else if (srcIsPat && !dstIsPat) || (dstIsPat && !srcIsPat) {
		return fmt.Errorf("both src and dst must be pattern if one is pattern")
	}
	return nil
}

func NewRefspec(s string) (*Refspec, error) {
	rs := &Refspec{}
	err := rs.UnmarshalText([]byte(s))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func MustRefspec(s string) *Refspec {
	rs := &Refspec{}
	err := rs.UnmarshalText([]byte(s))
	if err != nil {
		panic(err.Error())
	}
	return rs
}
