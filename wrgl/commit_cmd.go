// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
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
			c, err := utils.AggregateConfig(rd.FullPath)
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

func getRepoDir(cmd *cobra.Command) *utils.RepoDir {
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
	rd := utils.NewRepoDir(wrglDir, badgerLogInfo, badgerLogDebug)
	return rd
}

func quitIfRepoDirNotExist(cmd *cobra.Command, rd *utils.RepoDir) {
	if !rd.Exist() {
		cmd.PrintErrf("Repository not initialized in directory \"%s\". Initialize with command:\n", rd.FullPath)
		cmd.PrintErrln("  wrgl init")
		os.Exit(1)
	}
}

func ensureUserSet(cmd *cobra.Command, c *conf.Config) {
	out := cmd.ErrOrStderr()
	if c.User == nil || c.User.Email == "" {
		fmt.Fprintln(out, "User config not set. Set your user config with like this:")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, `  wrgl config --global user.email "john-doe@domain.com"`)
		fmt.Fprintln(out, `  wrgl config --global user.name "John Doe"`)
		os.Exit(1)
	}
}

func commit(cmd *cobra.Command, csvFilePath, message, branchName string, primaryKey []string, numWorkers int, rd *utils.RepoDir, c *conf.Config) error {
	if !ref.HeadPattern.MatchString(branchName) {
		return fmt.Errorf("invalid repo name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	db, err := rd.OpenObjectsStore()
	if err != nil {
		return err
	}
	defer db.Close()
	rs := rd.OpenRefStore()

	parent, _ := ref.GetHead(rs, branchName)

	var f io.ReadCloser
	var name string
	if csvFilePath == "-" {
		f = io.NopCloser(cmd.InOrStdin())
		name = "stdin"
	} else {
		file, err := os.Open(csvFilePath)
		if err != nil {
			return err
		}
		defer file.Close()
		f = file
		name = file.Name()
	}

	sum, err := ingest.IngestTable(db, f, name, primaryKey, 0, numWorkers, cmd.OutOrStdout())
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
	buf := bytes.NewBuffer(nil)
	_, err = commit.WriteTo(buf)
	if err != nil {
		return err
	}
	commitSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return err
	}
	err = ref.CommitHead(rs, branchName, commitSum, commit)
	if err != nil {
		return err
	}
	cmd.Printf("[%s %s] %s\n", branchName, hex.EncodeToString(commitSum)[:7], message)
	return nil
}
