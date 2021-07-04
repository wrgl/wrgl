// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

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
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			return writeCommitLog(cmd, db, rs, branchName, out)
		},
	}
	cmd.Flags().BoolP("no-pager", "P", false, "don't use PAGER")
	return cmd
}

func writeCommitLog(cmd *cobra.Command, db objects.Store, rs ref.Store, branchName string, out io.Writer) error {
	commitSum, err := ref.GetHead(rs, branchName)
	if err != nil {
		return err
	}
	hash := commitSum
	for {
		c, err := objects.GetCommit(db, hash)
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
