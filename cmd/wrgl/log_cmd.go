// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newLogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log BRANCH_NAME",
		Short: "Show commits log for a branch.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "displays logs in PAGER",
				Line:    "wrgl log main",
			},
			{
				Comment: "print logs to stdout",
				Line:    "wrgl log main --no-pager",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]
			out, cleanOut, err := utils.PagerOrOut(cmd)
			if err != nil {
				return err
			}
			defer cleanOut()
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
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
	f, err := utils.NewRemoteFinder(db, rs)
	if err != nil {
		return err
	}
	defer f.Close()
	zone, offset := time.Now().Zone()
	for {
		c, err := objects.GetCommit(db, hash)
		if err != nil {
			return err
		}
		tblExist := objects.TableExist(db, c.Table)
		var tblRemote string
		if !tblExist {
			if s, err := f.FindRemoteFor(c.Sum); err != nil {
				return err
			} else if s != "" {
				tblRemote = s
			}
		}
		fmt.Fprintf(out, "commit %s\n", hex.EncodeToString(hash))
		fmt.Fprintf(out, "table %x", c.Table)
		if tblExist {
			fmt.Fprintln(out)
		} else {
			c := color.New(color.FgRed)
			c.Fprint(out, " <missing")
			if tblRemote != "" {
				c.Fprintf(out, ", possibly reside on %s", tblRemote)
			}
			c.Fprint(out, ">\n")
		}
		fmt.Fprintf(out, "Author: %s <%s>\n", c.AuthorName, c.AuthorEmail)
		fmt.Fprintf(out, "Date: %s\n", c.Time.In(time.FixedZone(zone, offset)))
		fmt.Fprintf(out, "\n    %s\n\n", c.Message)
		if len(c.Parents) == 0 {
			break
		}
		hash = c.Parents[0]
	}
	return nil
}
