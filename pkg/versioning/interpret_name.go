package versioning

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

var (
	HeadPattern  = regexp.MustCompile(`^[-_0-9a-zA-Z/]+$`)
	HashPattern  = regexp.MustCompile(`^[a-f0-9]{32}$`)
	peelPattern  = regexp.MustCompile(`^(.*[^\^])(\^+)$`)
	tildePattern = regexp.MustCompile(`^(.*[^\~])\~(\d+)$`)
)

func parseNavigationChars(commitStr string) (commitName string, numPeel int, err error) {
	commitName = commitStr
	if peelPattern.MatchString(commitStr) {
		sl := peelPattern.FindStringSubmatch(commitStr)
		commitName = sl[1]
		numPeel = len(sl[2])
	} else if tildePattern.MatchString(commitStr) {
		sl := tildePattern.FindStringSubmatch(commitStr)
		commitName = sl[1]
		numPeel, err = strconv.Atoi(sl[2])
	}
	return
}

func peelCommit(db kv.DB, hash []byte, commit *objects.Commit, numPeel int) ([]byte, *objects.Commit, error) {
	var err error
	for numPeel > 0 {
		hash = commit.Parents[0][:]
		commit, err = GetCommit(db, hash)
		if err != nil {
			return nil, nil, err
		}
		numPeel--
	}
	return hash, commit, nil
}

func interpretRef(db kv.DB, name string, excludeTag bool) (ref string, sum []byte, err error) {
	m, err := ListAllRefs(db)
	if err != nil {
		return
	}
	sl := make([]string, 0, len(m))
	for k := range m {
		if excludeTag && strings.HasPrefix(k, "refs/tags/") {
			continue
		}
		sl = append(sl, k)
	}
	sort.Slice(sl, func(i, j int) bool {
		if strings.HasPrefix(sl[i], "refs/heads/") {
			if strings.HasPrefix(sl[j], "refs/heads/") {
				return sl[i] < sl[j]
			}
			return true
		} else if strings.HasPrefix(sl[i], "refs/tags/") {
			if strings.HasPrefix(sl[j], "refs/heads/") {
				return false
			} else if strings.HasPrefix(sl[j], "refs/tags/") {
				return sl[i] < sl[j]
			}
			return true
		} else if strings.HasPrefix(sl[i], "refs/remotes/") {
			if strings.HasPrefix(sl[j], "refs/heads/") || strings.HasPrefix(sl[j], "refs/tags/") {
				return false
			} else if strings.HasPrefix(sl[j], "refs/remotes/") {
				return sl[i] < sl[j]
			}
			return true
		}
		if strings.HasPrefix(sl[j], "refs/heads/") || strings.HasPrefix(sl[j], "refs/tags/") || strings.HasPrefix(sl[j], "refs/remotes/") {
			return false
		}
		return sl[i] < sl[j]
	})
	for _, ref := range sl {
		if ref == name || strings.HasSuffix(ref, "/"+name) {
			sum, err := GetRef(db, ref[5:])
			if err != nil {
				return name, nil, err
			}
			return ref, sum, nil
		}
	}
	return name, nil, kv.KeyNotFoundError
}

func InterpretCommitName(db kv.DB, commitStr string, excludeTag bool) (name string, hash []byte, commit *objects.Commit, err error) {
	name, numPeel, err := parseNavigationChars(commitStr)
	if err != nil {
		return "", nil, nil, err
	}
	if HashPattern.MatchString(name) {
		hash, err = hex.DecodeString(name)
		if err != nil {
			return
		}
		commit, err = GetCommit(db, hash)
		if err == nil {
			hash, commit, err = peelCommit(db, hash, commit, numPeel)
			name = hex.EncodeToString(hash)
			return
		}
	}
	if HeadPattern.MatchString(name) {
		var commitSum []byte
		name, commitSum, err = interpretRef(db, name, excludeTag)
		if err == nil {
			commit, err = GetCommit(db, commitSum)
			if err != nil {
				return "", nil, nil, err
			}
			hash, commit, err = peelCommit(db, commitSum, commit, numPeel)
			return
		}
	}
	if HashPattern.MatchString(name) {
		return "", nil, nil, fmt.Errorf("can't find commit %s", name)
	}
	return "", nil, nil, fmt.Errorf("can't find branch %s", name)
}
