package main

import (
	"encoding/csv"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/encoding"
)

func newExportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export COMMIT",
		Short: "Output commit content as CSV",
		Args:  cobra.ExactArgs(1),
		Example: strings.Join([]string{
			`  # export latest commit to CSV file`,
			`  wrgl export my-branch > my_branch.csv`,
			"",
			`  # export commit to CSV file`,
			`  wrgl export 1a2ed6248c7243cdaaecb98ac12213a7 > my_branch.csv`,
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr := args[0]
			return exportCommit(cmd, cStr)
		},
	}
	return cmd
}

func exportCommit(cmd *cobra.Command, cStr string) error {
	rd := getRepoDir(cmd)
	kvStore, err := rd.OpenKVStore()
	if err != nil {
		return err
	}
	defer kvStore.Close()
	fs := rd.OpenFileStore()

	_, _, commit, err := getCommit(cmd, kvStore, nil, cStr)
	if err != nil {
		return err
	}
	ts, err := commit.GetTable(kvStore, fs, seed)
	if err != nil {
		return err
	}
	reader, err := ts.NewRowReader()
	if err != nil {
		return err
	}
	writer := csv.NewWriter(cmd.OutOrStdout())
	defer writer.Flush()
	writer.Write(ts.Columns())
	for {
		_, rowContent, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		row, err := encoding.DecodeStrings(rowContent)
		if err != nil {
			return err
		}
		writer.Write(row)
	}
	return nil
}
