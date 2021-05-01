package main

import (
	"encoding/csv"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/versioning"
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
	ts, err := versioning.GetTable(kvStore, fs, seed, commit)
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
	dec := objects.NewStrListDecoder(true)
	for {
		_, rowContent, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = writer.Write(dec.Decode(rowContent))
		if err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}
