package versioning

import (
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

var (
	HeadPattern  = regexp.MustCompile(`^[-_0-9a-zA-Z]+$`)
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

func InterpretCommitName(db kv.DB, commitStr string) (hash []byte, commit *objects.Commit, file *os.File, err error) {
	commitName, numPeel, err := parseNavigationChars(commitStr)
	if err != nil {
		return nil, nil, nil, err
	}
	if db != nil && HashPattern.MatchString(commitName) {
		hash, err = hex.DecodeString(commitName)
		if err != nil {
			return
		}
		commit, err = GetCommit(db, hash)
		if err == nil {
			hash, commit, err = peelCommit(db, hash, commit, numPeel)
			return
		}
	}
	if db != nil && HeadPattern.MatchString(commitName) {
		var commitSum []byte
		commitSum, err = GetHead(db, commitName)
		if err == nil {
			hash = commitSum[:]
			commit, err = GetCommit(db, hash)
			if err != nil {
				return nil, nil, nil, err
			}
			hash, commit, err = peelCommit(db, hash, commit, numPeel)
			return
		}
	}
	file, err = os.Open(commitName)
	if err == nil {
		return nil, nil, file, err
	}
	if db != nil && HashPattern.MatchString(commitName) {
		return nil, nil, nil, fmt.Errorf("can't find commit %s", commitName)
	} else if db != nil && HeadPattern.MatchString(commitName) {
		return nil, nil, nil, fmt.Errorf("can't find branch %s", commitName)
	}
	return nil, nil, nil, fmt.Errorf("can't find file %s", commitName)
}
