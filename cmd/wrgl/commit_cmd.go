// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
				Comment: "commit from stdin",
				Line:    "cat data.csv | wrgl commit main - \"my commit\" -p id",
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
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := ensureUserSet(cmd, c); err != nil {
				return err
			}
			var branchName, csvFilePath, message string
			commitFromBranchFile := false
			if len(args) == 2 {
				branchName = args[0]
				message = args[1]
				errFileNotSet := fmt.Errorf("branch.file is not set for branch %q. You need to specify CSV_FILE_PATH", branchName)
				if c.Branch == nil {
					return errFileNotSet
				}
				if branch, ok := c.Branch[branchName]; !ok {
					return errFileNotSet
				} else if branch.File == "" {
					return errFileNotSet
				} else {
					csvFilePath = branch.File
					if len(primaryKey) == 0 && branch.PrimaryKey != nil {
						primaryKey = branch.PrimaryKey
					}
					commitFromBranchFile = true
				}
			} else {
				branchName = args[0]
				csvFilePath = args[1]
				message = args[2]
				if setFile && csvFilePath == "-" {
					return fmt.Errorf("can't set branch.file while commiting from stdin")
				}
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			var sum []byte
			if commitFromBranchFile {
				sum, err = commitIfBranchFileHasChanged(cmd, db, rs, c, branchName, csvFilePath, primaryKey, message, numWorkers, memLimit)
				if err != nil {
					return err
				}
				if sum == nil {
					cmd.Printf("file %s hasn't changed since the last commit. Aborting.\n", csvFilePath)
					return nil
				}
			} else {
				sum, err = commit(cmd, db, rs, csvFilePath, message, branchName, primaryKey, numWorkers, memLimit, c, debugFile)
				if err != nil {
					return err
				}
			}
			cmd.Printf("[%s %s] %s\n", branchName, hex.EncodeToString(sum)[:7], message)
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

func quitIfRepoDirNotExist(cmd *cobra.Command, rd *local.RepoDir) error {
	if !rd.Exist() {
		return fmt.Errorf(strings.Join([]string{
			fmt.Sprintf("Repository not initialized in directory \"%s\". Initialize with command:", rd.FullPath),
			"",
			"  wrgl init",
		}, "\n"))
	}
	return nil
}

func ensureUserSet(cmd *cobra.Command, c *conf.Config) error {
	if c.User == nil || c.User.Email == "" {
		return fmt.Errorf(strings.Join([]string{
			"User config not set. Set your user config with these commands:",
			"",
			`  wrgl config set --global user.email "john-doe@domain.com"`,
			`  wrgl config set --global user.name "John Doe"`,
		}, "\n"))
	}
	return nil
}

func commit(cmd *cobra.Command, db objects.Store, rs ref.Store, csvFilePath, message, branchName string, primaryKey []string, numWorkers int, memLimit uint64, c *conf.Config, debugFile io.Writer) ([]byte, error) {
	if !ref.HeadPattern.MatchString(branchName) {
		return nil, fmt.Errorf("invalid branch name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	parent, _ := ref.GetHead(rs, branchName)

	var f io.ReadCloser
	if csvFilePath == "-" {
		f = io.NopCloser(cmd.InOrStdin())
	} else {
		file, err := os.Open(csvFilePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		f = file
	}

	sortPT, blkPT := displayCommitProgress(cmd)
	s, err := sorter.NewSorter(memLimit, sortPT)
	if err != nil {
		return nil, err
	}
	sum, err := ingest.IngestTable(db, s, f, primaryKey,
		ingest.WithNumWorkers(numWorkers),
		ingest.WithProgressBar(blkPT),
		ingest.WithDebugOutput(debugFile),
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	commitSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return nil, err
	}
	err = ref.CommitHead(rs, branchName, commitSum, commit)
	if err != nil {
		return nil, err
	}
	return commitSum, nil
}

func commitTempBranch(cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, tmpBranch, csvFilePath string, primaryKey []string, numWorkers int, memLimit uint64) (sum []byte, err error) {
	ref.DeleteHead(rs, tmpBranch)
	return commit(cmd, db, rs, csvFilePath, filepath.Base(csvFilePath), tmpBranch, primaryKey, numWorkers, memLimit, c, nil)
}

func ensureTempCommit(cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, branch string, csvFilePath string, primaryKey []string, numWorkers int, memLimit uint64) (sum []byte, err error) {
	tmpBranch := branch + "-tmp"
	sum, err = ref.GetHead(rs, tmpBranch)
	if err == ref.ErrKeyNotFound {
		sum, err = commitTempBranch(cmd, db, rs, c, tmpBranch, csvFilePath, primaryKey, numWorkers, memLimit)
		if err != nil {
			return nil, err
		}
		return sum, nil
	}
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		return nil, err
	}
	fd, err := os.Stat(csvFilePath)
	if err != nil {
		return nil, err
	}
	if com.Message != fd.Name() || com.Time.Before(fd.ModTime()) {
		sum, err = commitTempBranch(cmd, db, rs, c, tmpBranch, csvFilePath, primaryKey, numWorkers, memLimit)
		if err != nil {
			return nil, err
		}
		return sum, nil
	}
	return sum, nil
}

func commitWithTable(cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, branch string, tableSum []byte, message string) ([]byte, error) {
	parent, _ := ref.GetHead(rs, branch)
	commit := &objects.Commit{
		Table:       tableSum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: c.User.Email,
		AuthorName:  c.User.Name,
	}
	if parent != nil {
		commit.Parents = [][]byte{parent}
	}
	buf := bytes.NewBuffer(nil)
	_, err := commit.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	commitSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return nil, err
	}
	err = ref.CommitHead(rs, branch, commitSum, commit)
	if err != nil {
		return nil, err
	}
	return commitSum, nil
}

func commitIfBranchFileHasChanged(cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, branch string, csvFilePath string, primaryKey []string, message string, numWorkers int, memLimit uint64) ([]byte, error) {
	tmpSum, err := ensureTempCommit(cmd, db, rs, c, branch, csvFilePath, primaryKey, numWorkers, memLimit)
	if err != nil {
		return nil, err
	}
	tmpCom, err := objects.GetCommit(db, tmpSum)
	if err != nil {
		return nil, err
	}
	oldSum, _ := ref.GetHead(rs, branch)
	if oldSum != nil {
		oldCom, err := objects.GetCommit(db, oldSum)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(tmpCom.Table, oldCom.Table) {
			return nil, nil
		}
	}
	return commitWithTable(cmd, c, db, rs, branch, tmpCom.Table, message)
}
