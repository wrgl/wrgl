// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit BRANCH CSV_FILE_PATH MESSAGE",
		Short: "Commit CSV file to a repo",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]
			csvFilePath := args[1]
			message := args[2]
			primaryKey, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			numWorkers, err := cmd.Flags().GetInt("num-workers")
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			c, err := versioning.AggregateConfig(rd.FullPath)
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			return commit(cmd, csvFilePath, message, branchName, primaryKey, numWorkers, rd, c)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	return cmd
}

func getRepoDir(cmd *cobra.Command) *versioning.RepoDir {
	wrglDir := utils.MustWRGLDir(cmd)
	badgerLogInfo, err := cmd.Flags().GetBool("badger-log-info")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	badgerLogDebug, err := cmd.Flags().GetBool("badger-log-debug")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	rd := versioning.NewRepoDir(wrglDir, badgerLogInfo, badgerLogDebug)
	return rd
}

func quitIfRepoDirNotExist(cmd *cobra.Command, rd *versioning.RepoDir) {
	if !rd.Exist() {
		cmd.PrintErrf("Repository not initialized in directory \"%s\". Initialize with command:\n", rd.FullPath)
		cmd.PrintErrln("  wrgl init")
		os.Exit(1)
	}
}

func ensureUserSet(cmd *cobra.Command, c *versioning.Config) {
	out := cmd.ErrOrStderr()
	if c.User == nil || c.User.Email == "" {
		fmt.Fprintln(out, "User config not set. Set your user config with like this:")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, `  wrgl config --global user.email "john-doe@domain.com"`)
		fmt.Fprintln(out, `  wrgl config --global user.name "John Doe"`)
		os.Exit(1)
	}
}

func ingestTable(cmd *cobra.Command, db kv.DB, fs kv.FileStore, numWorkers int, f io.Reader, primaryKey []string) ([]byte, error) {
	csvReader, columns, primaryKeyIndices, err := ingest.ReadColumns(f, primaryKey)
	if err != nil {
		return nil, err
	}
	tb := table.NewBuilder(db, fs, columns, primaryKeyIndices, seed, 0)
	return ingest.NewIngestor(tb, seed, primaryKeyIndices, numWorkers, cmd.OutOrStdout()).
		ReadRowsFromCSVReader(csvReader).
		Ingest()
}

func commit(cmd *cobra.Command, csvFilePath, message, branchName string, primaryKey []string, numWorkers int, rd *versioning.RepoDir, c *versioning.Config) error {
	if !versioning.HeadPattern.MatchString(branchName) {
		return fmt.Errorf("invalid repo name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	kvStore, err := rd.OpenKVStore()
	if err != nil {
		return err
	}
	defer kvStore.Close()
	fs := rd.OpenFileStore()

	parent, _ := versioning.GetHead(kvStore, branchName)

	var f io.Reader
	if csvFilePath == "-" {
		f = cmd.InOrStdin()
	} else {
		file, err := os.Open(csvFilePath)
		if err != nil {
			return err
		}
		defer file.Close()
		f = file
	}

	sum, err := ingestTable(cmd, kvStore, fs, numWorkers, f, primaryKey)
	if err != nil {
		return err
	}

	commit := &objects.Commit{
		Table:       sum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: c.User.Email,
		AuthorName:  c.User.Name,
	}
	if parent != nil {
		commit.Parents = [][]byte{parent}
	}
	commitSum, err := versioning.SaveCommit(kvStore, seed, commit)
	if err != nil {
		return err
	}
	err = versioning.CommitHead(kvStore, fs, branchName, commitSum, commit)
	if err != nil {
		return err
	}
	cmd.Printf("[%s %s] %s\n", branchName, hex.EncodeToString(commitSum)[:7], message)
	return nil
}
