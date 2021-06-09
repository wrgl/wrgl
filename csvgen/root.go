// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "csvgen CSV_FILE",
		Short: "Modify given CSV and output to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			addRemCols, err := cmd.Flags().GetBool("addrem-cols")
			if err != nil {
				return err
			}
			renameCols, err := cmd.Flags().GetBool("rename-cols")
			if err != nil {
				return err
			}
			moveCols, err := cmd.Flags().GetBool("move-cols")
			if err != nil {
				return err
			}
			modRows, err := cmd.Flags().GetBool("mod-rows")
			if err != nil {
				return err
			}
			rows, err := readCSV(args[0])
			if err != nil {
				return err
			}
			modifiedCols := map[string]struct{}{}
			numColMods := oneFifth(len(rows[0]))
			if addRemCols {
				rows = addColumns(modifiedCols, numColMods, rows)
				rows = remColumns(modifiedCols, numColMods, rows)
			}
			if renameCols {
				rows = renameColumns(modifiedCols, numColMods, rows)
			}
			if moveCols {
				rows = moveColumns(modifiedCols, numColMods, rows)
			}
			if modRows {
				modifiedRows := map[int]struct{}{}
				numRowMods := oneFifth(len(rows) - 1)
				rows = addRows(modifiedRows, numRowMods, rows)
				rows = removeRows(modifiedRows, numRowMods, rows)
				rows = modifyRows(modifiedRows, numRowMods, rows)
			}
			w := csv.NewWriter(cmd.OutOrStdout())
			return w.WriteAll(rows)
		},
	}
	cmd.Flags().Bool("addrem-cols", false, "Randomly add and remove columns")
	cmd.Flags().Bool("rename-cols", false, "Randomly rename columns")
	cmd.Flags().Bool("mod-rows", false, "Randomly add, remove and modify rows")
	cmd.Flags().Bool("move-cols", false, "Randomly move columns")
	return cmd
}

func readCSV(name string) (rows [][]string, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err = r.ReadAll()
	if err != nil {
		return
	}
	if len(rows[0]) < 5 {
		return nil, fmt.Errorf("original file has too few columns, try to pass in file with minimum 5 columns")
	}
	rows[0] = genColumns(len(rows[0]))
	return
}
