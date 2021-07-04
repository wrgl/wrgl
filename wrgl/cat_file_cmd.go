// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"

	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/widgets"
)

func newCatFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat-file OBJECT_HASH",
		Short: "Provide content information for repository object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
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
			blk, err := objects.GetBlock(db, hash)
			if err == nil {
				return catBlock(cmd, blk)
			}
			return fmt.Errorf("unrecognized hash")
		},
	}
	return cmd
}

func catCommit(cmd *cobra.Command, commit *objects.Commit) error {
	app := tview.NewApplication()
	textView := tview.NewTextView().
		SetDynamicColors(true)
	fmt.Fprintf(textView, "[yellow]table[white]  %s\n", hex.EncodeToString(commit.Table))
	fmt.Fprintf(textView, "[yellow]author[white] %s <%s>\n", commit.AuthorName, commit.AuthorEmail)
	fmt.Fprintf(textView, "[yellow]time[white]   %d %s\n\n", commit.Time.Unix(), commit.Time.Format("-0700"))
	fmt.Fprintln(textView, commit.Message)
	return app.SetRoot(textView, true).SetFocus(textView).Run()
}

func catTable(cmd *cobra.Command, tbl *objects.Table) error {
	cols := tbl.Columns
	pk := tbl.PrimaryKey()
	app := tview.NewApplication()
	textView := widgets.NewPaginatedTextView().
		SetDynamicColors(true)
	fmt.Fprintf(textView, "[yellow]columns[white] ([cyan]%d[white])\n\n", len(cols))
	for _, col := range cols {
		fmt.Fprintf(textView, "%s\n", col)
	}
	if len(pk) > 0 {
		fmt.Fprintf(textView, "\n[yellow]primary key[white] ([cyan]%d[white])\n\n", len(pk))
		for _, col := range pk {
			fmt.Fprintf(textView, "%s\n", col)
		}
	}
	fmt.Fprintf(textView, "\n[yellow]rows[white] ([cyan]%d[white])\n\n", tbl.RowsCount)
	err := textView.PullText()
	if err != nil {
		return err
	}
	for _, blk := range tbl.Blocks {
		fmt.Fprintf(textView, "[aquaMarine]%x[white]\n", blk)
	}
	return app.SetRoot(textView, true).SetFocus(textView).Run()
}

func catBlock(cmd *cobra.Command, block [][]string) error {
	w := csv.NewWriter(cmd.OutOrStdout())
	return w.WriteAll(block)
}
