package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/versioning"
)

func getKVStore(cmd *cobra.Command) (kv.Store, error) {
	rd := getRepoDir(cmd)
	quitIfRepoDirNotExist(cmd, rd)
	return rd.OpenKVStore()
}

func newLogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log BRANCH_NAME",
		Short: "Shows commits log",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]
			out, cleanOut, err := pagerOrOut(cmd)
			if err != nil {
				return err
			}
			defer cleanOut()
			kvStore, err := getKVStore(cmd)
			if err != nil {
				return err
			}
			return writeCommitLog(cmd, kvStore, branchName, out)
		},
	}
	cmd.Flags().BoolP("no-pager", "P", false, "don't use PAGER")
	return cmd
}

func writeCommitLog(cmd *cobra.Command, kvStore kv.Store, branchName string, out io.Writer) error {
	branch, err := versioning.GetBranch(kvStore, branchName)
	if err != nil {
		return err
	}
	hash := branch.CommitHash
	for hash != "" {
		c, err := versioning.GetCommit(kvStore, hash)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "commit %s\n", hash)
		fmt.Fprintf(out, "Author: %s <%s>\n", c.Author.Name, c.Author.Email)
		fmt.Fprintf(out, "Date: %s\n", c.Timestamp)
		fmt.Fprintf(out, "\n    %s\n\n", c.Message)
		hash = c.PrevCommitHash
	}
	return nil
}
