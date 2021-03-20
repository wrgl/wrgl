package main

import (
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/versioning"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff COMMIT1 COMMIT2",
		Short: "Shows diff between 2 commits",
		Example: strings.Join([]string{
			`  wrgl diff branch-1 branch-1^`,
			`  wrgl diff branch-1 branch-2`,
			`  wrgl diff branch-1 1a2ed6248c7243cdaaecb98ac12213a7`,
			`  wrgl diff 1a2ed6248c7243cdaaecb98ac12213a7 f1cf51efa2c1e22843b0e083efd89792`,
		}, "\n"),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr1 := args[0]
			cStr2 := args[1]
			return diffCommits(cmd, cStr1, cStr2)
		},
	}
	return cmd
}

var hashPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

func getCommit(db kv.DB, cStr string) (string, *versioning.Commit, error) {
	var hash = cStr
	if !hashPattern.MatchString(cStr) {
		branch, err := versioning.GetBranch(db, cStr)
		if err != nil {
			return "", nil, err
		}
		hash = branch.CommitHash
	}
	commit, err := versioning.GetCommit(db, hash)
	if err != nil {
		return "", nil, err
	}
	return hash, commit, nil
}

func diffCommits(cmd *cobra.Command, cStr1, cStr2 string) error {
	rd := getRepoDir(cmd)
	kvStore, err := rd.OpenKVStore()
	if err != nil {
		return err
	}
	defer kvStore.Close()
	fs := rd.OpenFileStore()
	_, commit1, err := getCommit(kvStore, cStr1)
	if err != nil {
		return err
	}
	_, commit2, err := getCommit(kvStore, cStr2)
	if err != nil {
		return err
	}
	ts1, err := commit1.GetTable(kvStore, fs, seed)
	if err != nil {
		return err
	}
	ts2, err := commit2.GetTable(kvStore, fs, seed)
	if err != nil {
		return err
	}
	diffChan := make(chan diff.DiffEvent)
	defer close(diffChan)

	return diff.DiffTables(ts1, ts2, diffChan, 65*time.Millisecond)
}
