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
	BranchPattern = regexp.MustCompile(`^[-_0-9a-zA-Z]+$`)
	HashPattern   = regexp.MustCompile(`^[a-f0-9]{32}$`)
	hatPattern    = regexp.MustCompile(`^(.*[^\^])(\^+)$`)
	tildePattern  = regexp.MustCompile(`^(.*[^\~])\~(\d+)$`)
)

func parseNavigationChars(commitStr string) (commitName string, goBack int, err error) {
	commitName = commitStr
	if hatPattern.MatchString(commitStr) {
		sl := hatPattern.FindStringSubmatch(commitStr)
		commitName = sl[1]
		goBack = len(sl[2])
	} else if tildePattern.MatchString(commitStr) {
		sl := tildePattern.FindStringSubmatch(commitStr)
		commitName = sl[1]
		goBack, err = strconv.Atoi(sl[2])
	}
	return
}

func getPrevCommit(db kv.DB, hash []byte, commit *objects.Commit, goBack int) ([]byte, *objects.Commit, error) {
	var err error
	for goBack > 0 {
		hash = commit.PrevCommitSum
		commit, err = GetCommit(db, hash)
		if err != nil {
			return nil, nil, err
		}
		goBack--
	}
	return hash, commit, nil
}

func InterpretCommitName(db kv.DB, commitStr string) (hash []byte, commit *objects.Commit, file *os.File, err error) {
	commitName, goBack, err := parseNavigationChars(commitStr)
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
			hash, commit, err = getPrevCommit(db, hash, commit, goBack)
			return
		}
	}
	if db != nil && BranchPattern.MatchString(commitName) {
		var branch *objects.Branch
		branch, err = GetBranch(db, commitName)
		if err == nil {
			hash = branch.CommitSum
			commit, err = GetCommit(db, hash)
			if err != nil {
				return nil, nil, nil, err
			}
			hash, commit, err = getPrevCommit(db, hash, commit, goBack)
			return
		}
	}
	file, err = os.Open(commitName)
	if err == nil {
		return nil, nil, file, err
	}
	if db != nil && HashPattern.MatchString(commitName) {
		return nil, nil, nil, fmt.Errorf("can't find commit %s", commitName)
	} else if db != nil && BranchPattern.MatchString(commitName) {
		return nil, nil, nil, fmt.Errorf("can't find branch %s", commitName)
	}
	return nil, nil, nil, fmt.Errorf("can't find file %s", commitName)
}
