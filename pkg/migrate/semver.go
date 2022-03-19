package migrate

import (
	"fmt"
	"strconv"
	"strings"
)

type SemVer struct {
	Major int
	Minor int
	Patch int
}

func (v *SemVer) UnmarshalText(text []byte) (err error) {
	parts := strings.Split(string(text), ".")
	if len(parts) != 3 {
		return fmt.Errorf("invalid semver")
	}
	for i, v := range []*int{&v.Major, &v.Minor, &v.Patch} {
		*v, err = strconv.Atoi(parts[i])
		if err != nil {
			return
		}
	}
	return nil
}

func (v *SemVer) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func (v *SemVer) MarshalText() (text []byte, err error) {
	return []byte(v.String()), nil
}

func (a *SemVer) CompareTo(b *SemVer) int {
	if a.Major < b.Major {
		return -1
	} else if a.Major > b.Major {
		return 1
	}
	if a.Minor < b.Minor {
		return -1
	} else if a.Minor > b.Minor {
		return 1
	}
	if a.Patch < b.Patch {
		return -1
	} else if a.Patch > b.Patch {
		return 1
	}
	return 0
}

func ParseSemVer(s string) (*SemVer, error) {
	v := &SemVer{}
	if err := v.UnmarshalText([]byte(s)); err != nil {
		return nil, err
	}
	return v, nil
}
