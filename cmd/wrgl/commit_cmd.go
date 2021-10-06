// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit BRANCH CSV_FILE_PATH COMMIT_MESSAGE",
		Short: "Commit a CSV file under a branch",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "commit using primary key id",
				Line:    "wrgl commit main data.csv \"initial commit\" -p id",
			},
			{
				Comment: "commit using composite primary key",
				Line:    "wrgl commit main data.csv \"new data\" -p id,date",
			},
		}),
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			debugFile, cleanup, err := setupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
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
			memLimit, err := cmd.Flags().GetUint64("mem-limit")
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			quitIfRepoDirNotExist(cmd, rd)
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			return commit(cmd, csvFilePath, message, branchName, primaryKey, numWorkers, memLimit, rd, c, debugFile)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize")
	cmd.Flags().Uint64("mem-limit", 0, "limit memory consumption (in bytes). If not set then memory limit is automatically calculated.")
	return cmd
}

func quitIfRepoDirNotExist(cmd *cobra.Command, rd *local.RepoDir) {
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

func commit(cmd *cobra.Command, csvFilePath, message, branchName string, primaryKey []string, numWorkers int, memLimit uint64, rd *local.RepoDir, c *conf.Config, debugFile io.Writer) error {
	if !ref.HeadPattern.MatchString(branchName) {
		return fmt.Errorf("invalid branch name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	db, err := rd.OpenObjectsStore()
	if err != nil {
		return err
	}
	defer db.Close()
	rs := rd.OpenRefStore()

	parent, _ := ref.GetHead(rs, branchName)

	var f io.ReadCloser
	if csvFilePath == "-" {
		f = io.NopCloser(cmd.InOrStdin())
	} else {
		file, err := os.Open(csvFilePath)
		if err != nil {
			return err
		}
		defer file.Close()
		f = file
	}

	sortPT, blkPT := displayCommitProgress(cmd)
	sum, err := ingest.IngestTable(db, f, primaryKey,
		ingest.WithSortRunSize(memLimit),
		ingest.WithNumWorkers(numWorkers),
		ingest.WithSortProgressBar(sortPT),
		ingest.WithProgressBar(blkPT),
		ingest.WithDebugOutput(debugFile),
	)
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
