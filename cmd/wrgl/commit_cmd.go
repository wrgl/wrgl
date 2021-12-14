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
	"github.com/wrgl/wrgl/pkg/sorter"
)

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit BRANCH [CSV_FILE_PATH] COMMIT_MESSAGE [-p PRIMARY_KEY]",
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
			{
				Comment: "commit while setting branch.file and branch.primaryKey",
				Line:    "wrgl commit main data.csv \"my commit\" -p id --set-file --set-primary-key",
			},
			{
				Comment: "commit without having to specify CSV_FILE_PATH and PRIMARY_KEY (read from branch.file and branch.primaryKey)",
				Line:    "wrgl commit main \"easy commit\"",
			},
		}),
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			debugFile, cleanup, err := utils.SetupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
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
			setFile, err := cmd.Flags().GetBool("set-file")
			if err != nil {
				return err
			}
			setPK, err := cmd.Flags().GetBool("set-primary-key")
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
			var branchName, csvFilePath, message string
			if len(args) == 2 {
				branchName = args[0]
				message = args[1]
				if branch, ok := c.Branch[branchName]; !ok {
					return fmt.Errorf("no configuration found for branch %q. You need to specify CSV_FILE_PATH", branchName)
				} else if branch.File == "" {
					return fmt.Errorf("branch.file is not set for branch %q. You need to specify CSV_FILE_PATH", branchName)
				} else {
					csvFilePath = branch.File
					if len(primaryKey) == 0 && branch.PrimaryKey != nil {
						primaryKey = branch.PrimaryKey
					}
				}
			} else {
				branchName = args[0]
				csvFilePath = args[1]
				message = args[2]
			}
			if err := commit(cmd, csvFilePath, message, branchName, primaryKey, numWorkers, memLimit, rd, c, debugFile); err != nil {
				return err
			}
			if setFile || setPK {
				s := conffs.NewStore(rd.FullPath, conffs.LocalSource, "")
				c, err := s.Open()
				if err != nil {
					return err
				}
				if c.Branch == nil {
					c.Branch = map[string]*conf.Branch{}
				}
				if _, ok := c.Branch[branchName]; !ok {
					c.Branch[branchName] = &conf.Branch{}
				}
				if setFile {
					c.Branch[branchName].File = csvFilePath
				}
				if setPK {
					c.Branch[branchName].PrimaryKey = primaryKey
				}
				return s.Save(c)
			}
			return nil
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize")
	cmd.Flags().Uint64("mem-limit", 0, "limit memory consumption (in bytes). If not set then memory limit is automatically calculated.")
	cmd.Flags().Bool("set-file", false, "set branch.file to CSV_FILE_PATH. If branch.file is set then you don't need to specify CSV_FILE_PATH in subsequent commits to BRANCH.")
	cmd.Flags().Bool("set-primary-key", false, "set branch.primaryKey to PRIMARY_KEY. If branch.primaryKey is set then you don't need to specify PRIMARY_KEY in subsequent commits to BRANCH.")
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
		fmt.Fprintln(out, `  wrgl config set --global user.email "john-doe@domain.com"`)
		fmt.Fprintln(out, `  wrgl config set --global user.name "John Doe"`)
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
	s, err := sorter.NewSorter(memLimit, sortPT)
	if err != nil {
		return err
	}
	sum, err := ingest.IngestTable(db, s, f, primaryKey,
		ingest.WithNumWorkers(numWorkers),
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
