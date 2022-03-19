package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

type migration struct {
	SemVer  *SemVer
	Migrate func(dir string) error
}

var migrations []migration

func insertMigration(sl []migration, m migration) []migration {
	n := len(sl)
	if i := sort.Search(n, func(i int) bool {
		return sl[i].SemVer.CompareTo(m.SemVer) >= 0
	}); i <= n-1 {
		sl = append(sl[:i+1], sl[i:]...)
		sl[i] = m
	} else {
		sl = append(sl, m)
	}
	return sl
}

func readVersion(dir string) (*SemVer, error) {
	v := &SemVer{}
	f, err := os.Open(filepath.Join(dir, "version"))
	if err != nil {
		return v, nil
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if err = v.UnmarshalText(b); err != nil {
		return nil, err
	}
	return v, nil
}

func writeVersion(dir string, v *SemVer) error {
	f, err := os.Create(filepath.Join(dir, "version"))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(v.String()))
	return err
}

func Migrate(dir string) (err error) {
	semver, err := readVersion(dir)
	var newSemver *SemVer
	for _, m := range migrations {
		if m.SemVer.CompareTo(semver) <= 0 {
			continue
		}
		if err = m.Migrate(dir); err != nil {
			return fmt.Errorf("error while migrating to v%s: %v", m.SemVer, err)
		}
		newSemver = m.SemVer
	}
	if newSemver != nil {
		return writeVersion(dir, newSemver)
	}
	return nil
}
