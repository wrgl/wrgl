// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/csv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newExportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export COMMIT",
		Short: "Print commit content as CSV",
		Args:  cobra.ExactArgs(1),
		Example: strings.Join([]string{
			`  # export latest commit to CSV file`,
			`  wrgl export my-branch > my_branch.csv`,
			"",
			`  # export commit to CSV file`,
			`  wrgl export 1a2ed6248c7243cdaaecb98ac12213a7 > my_data.csv`,
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr := args[0]
			return exportCommit(cmd, cStr)
		},
	}
	cmd.Flags().String("delimiter", "", "CSV delimiter. Defaults to comma.")
	return cmd
}

func exportCommit(cmd *cobra.Command, cStr string) error {
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

	_, _, commit, err := ref.InterpretCommitName(db, rs, cStr, false)
	if err != nil {
		return err
	}
	tbl, err := utils.GetTable(db, rs, commit)
	if err != nil {
		return err
	}
	delim, err := getRuneFromFlag(cmd, "delimiter")
	if err != nil {
		return err
	}
	writer := csv.NewWriter(cmd.OutOrStdout())
	if delim != 0 {
		writer.Comma = delim
	}
	err = writer.Write(tbl.Columns)
	if err != nil {
		return err
	}
	var buf []byte
	var blk [][]string
	for _, sum := range tbl.Blocks {
		blk, buf, err = objects.GetBlock(db, buf, sum)
		if err != nil {
			return err
		}
		for _, row := range blk {
			err = writer.Write(row)
			if err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}
