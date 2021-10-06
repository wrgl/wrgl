// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/wrgl/wrgl/pkg/objects"
)

var (
	HeadPattern  = regexp.MustCompile(`^[-_0-9a-zA-Z/]+$`)
	HashPattern  = regexp.MustCompile(`^[a-f0-9]{32}$`)
	peelPattern  = regexp.MustCompile(`^(.*[^\^])(\^+)$`)
	tildePattern = regexp.MustCompile(`^(.*[^\~])\~(\d+)$`)
)

func ParseNavigationChars(commitStr string) (commitName string, numPeel int, err error) {
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

func PeelCommit(db objects.Store, hash []byte, commit *objects.Commit, numPeel int) ([]byte, *objects.Commit, error) {
	var err error
	for numPeel > 0 {
		hash = commit.Parents[0][:]
		commit, err = objects.GetCommit(db, hash)
		if err != nil {
			return nil, nil, err
		}
		numPeel--
	}
	return hash, commit, nil
}

func interpretRef(s Store, name string, excludeTag bool) (ref string, sum []byte, err error) {
	name = strings.TrimPrefix(name, "refs/")
	m, err := ListAllRefs(s)
	if err != nil {
		return
	}
	sl := make([]string, 0, len(m))
	for k := range m {
		if excludeTag && strings.HasPrefix(k, "tags/") {
			continue
		}
		sl = append(sl, k)
	}
	sort.Slice(sl, func(i, j int) bool {
		if strings.HasPrefix(sl[i], "heads/") {
			if strings.HasPrefix(sl[j], "heads/") {
				return sl[i] < sl[j]
			}
			return true
		} else if strings.HasPrefix(sl[i], "tags/") {
			if strings.HasPrefix(sl[j], "heads/") {
				return false
			} else if strings.HasPrefix(sl[j], "tags/") {
				return sl[i] < sl[j]
			}
			return true
		} else if strings.HasPrefix(sl[i], "remotes/") {
			if strings.HasPrefix(sl[j], "heads/") || strings.HasPrefix(sl[j], "tags/") {
				return false
			} else if strings.HasPrefix(sl[j], "remotes/") {
				return sl[i] < sl[j]
			}
			return true
		}
		if strings.HasPrefix(sl[j], "heads/") || strings.HasPrefix(sl[j], "tags/") || strings.HasPrefix(sl[j], "remotes/") {
			return false
		}
		return sl[i] < sl[j]
	})
	for _, ref := range sl {
		if ref == name || strings.HasSuffix(ref, "/"+name) {
			sum, err := GetRef(s, ref)
			if err != nil {
				return name, nil, err
			}
			return ref, sum, nil
		}
	}
	return name, nil, ErrKeyNotFound
}

func InterpretCommitName(db objects.Store, rs Store, commitStr string, excludeTag bool) (name string, hash []byte, commit *objects.Commit, err error) {
	name, numPeel, err := ParseNavigationChars(commitStr)
	if err != nil {
		return "", nil, nil, err
	}
	if HashPattern.MatchString(name) {
		hash, err = hex.DecodeString(name)
		if err != nil {
			return
		}
		commit, err = objects.GetCommit(db, hash)
		if err == nil {
			hash, commit, err = PeelCommit(db, hash, commit, numPeel)
			name = hex.EncodeToString(hash)
			return
		}
	}
	if HeadPattern.MatchString(name) {
		var commitSum []byte
		name, commitSum, err = interpretRef(rs, name, excludeTag)
		if err == nil {
			commit, err = objects.GetCommit(db, commitSum)
			if err != nil {
				return "", nil, nil, err
			}
			hash, commit, err = PeelCommit(db, commitSum, commit, numPeel)
			return
		}
	}
	if HashPattern.MatchString(name) {
		return "", nil, nil, fmt.Errorf("can't find commit %s", name)
	}
	return "", nil, nil, fmt.Errorf("can't find branch %s", name)
}
