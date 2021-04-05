package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
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
	quitIfRepoDirNotExist(cmd, rd)
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
	err = writer.Write(ts.Columns())
	if err != nil {
		return err
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		return err
	}
	for {
		_, rowContent, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Fprint(cmd.OutOrStdout(), string(rowContent))
	}
	return nil
}
