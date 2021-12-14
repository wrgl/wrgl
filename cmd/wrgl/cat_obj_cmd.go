// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"

	"github.com/mitchellh/colorstring"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
)

func newCatFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat-obj OBJECT_SUM",
		Short: "Print information for an object.",
		Long:  "Print information for an object. This command only work for 3 types of objects: commit, table, and block.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}
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
			commit, err := objects.GetCommit(db, hash)
			if err == nil {
				return catCommit(cmd, commit)
			}
			tbl, err := objects.GetTable(db, hash)
			if err == nil {
				return catTable(cmd, tbl)
			}
			blk, _, err := objects.GetBlock(db, nil, hash)
			if err == nil {
				return catBlock(cmd, blk)
			}
			return fmt.Errorf("unrecognized hash")
		},
	}
	return cmd
}

func catCommit(cmd *cobra.Command, commit *objects.Commit) error {
	out := cmd.OutOrStdout()
	colorstring.Fprintf(out, "[yellow]table[white]  %s\n", hex.EncodeToString(commit.Table))
	colorstring.Fprintf(out, "[yellow]author[white] %s <%s>\n", commit.AuthorName, commit.AuthorEmail)
	colorstring.Fprintf(out, "[yellow]time[white]   %d %s\n\n", commit.Time.Unix(), commit.Time.Format("-0700"))
	colorstring.Fprintln(out, commit.Message)
	return nil
}

func catTable(cmd *cobra.Command, tbl *objects.Table) error {
	out := cmd.OutOrStdout()
	cols := tbl.Columns
	pk := tbl.PrimaryKey()
	colorstring.Fprintf(out, "[yellow]columns[white] ([cyan]%d[white])\n\n", len(cols))
	for _, col := range cols {
		colorstring.Fprintf(out, "  %s\n", col)
	}
	if len(pk) > 0 {
		colorstring.Fprintf(out, "\n[yellow]primary key[white] ([cyan]%d[white])\n\n", len(pk))
		for _, col := range pk {
			colorstring.Fprintf(out, "  %s\n", col)
		}
	}
	colorstring.Fprintf(out, "\n[yellow]rows[white]: [cyan]%d[white]\n\n", tbl.RowsCount)
	colorstring.Fprintf(out, "[yellow]blocks[white] ([cyan]%d[white])\n\n", len(tbl.Blocks))
	for _, blk := range tbl.Blocks {
		colorstring.Fprintf(out, "  [white]%x\n", blk)
	}
	return nil
}

func catBlock(cmd *cobra.Command, block [][]string) error {
	w := csv.NewWriter(cmd.OutOrStdout())
	return w.WriteAll(block)
}
