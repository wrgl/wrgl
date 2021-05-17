package main

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
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
			out, cleanOut, err := utils.PagerOrOut(cmd)
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
	commitSum, err := versioning.GetHead(kvStore, branchName)
	if err != nil {
		return err
	}
	hash := commitSum
	for {
		c, err := versioning.GetCommit(kvStore, hash)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "commit %s\n", hex.EncodeToString(hash))
		fmt.Fprintf(out, "Author: %s <%s>\n", c.AuthorName, c.AuthorEmail)
		fmt.Fprintf(out, "Date: %s\n", c.Time)
		fmt.Fprintf(out, "\n    %s\n\n", c.Message)
		if len(c.Parents) == 0 {
			break
		}
		hash = c.Parents[0]
	}
	return nil
}
