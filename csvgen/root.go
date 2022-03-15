// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/csvmod"
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
			perserveCols, err := cmd.Flags().GetStringSlice("preserve-cols")
			if err != nil {
				return err
			}
			fraction, err := cmd.Flags().GetFloat64("fraction")
			if err != nil {
				return err
			}
			rows, err := readCSV(args[0])
			if err != nil {
				return err
			}
			m := csvmod.NewModifier(rows).
				PreserveColumns(perserveCols)
			if addRemCols {
				m.AddColumns(fraction).RemColumns(fraction)
			}
			if renameCols {
				m.RenameColumns(fraction)
			}
			if moveCols {
				m.MoveColumns(fraction)
			}
			if modRows {
				m.AddRows(fraction).RemoveRows(fraction).ModifyRows(fraction)
			}
			w := csv.NewWriter(cmd.OutOrStdout())
			return w.WriteAll(m.Rows)
		},
	}
	cmd.Flags().Bool("addrem-cols", false, "Randomly add and remove columns")
	cmd.Flags().Bool("rename-cols", false, "Randomly rename columns")
	cmd.Flags().Bool("mod-rows", false, "Randomly add, remove and modify rows")
	cmd.Flags().Bool("move-cols", false, "Randomly move columns")
	cmd.Flags().StringSlice("preserve-cols", nil, "preserve columns with these names")
	cmd.Flags().Float64P("fraction", "f", 0.2, "Fraction of rows/columns that will be modified. Defaults to 0.2")
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

func genColumns(n int) []string {
	cols := make([]string, n)
	for i := 0; i < n; i++ {
		col := []byte("col_")
		if i < 25 {
			col = append(col, byte(i+97))
			cols[i] = string(col)
			continue
		}
		chars := []byte{}
		for k := i; k > 0; k = k / 25 {
			chars = append(chars, byte(k-(k/25)*25))
		}
		for j := len(chars) - 1; j >= 0; j-- {
			col = append(col, chars[j]+97)
		}
		cols[i] = string(col)
	}
	return cols
}
