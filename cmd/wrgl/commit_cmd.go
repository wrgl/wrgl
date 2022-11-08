// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
)

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit {BRANCH | --all} [CSV_FILE_PATH] COMMIT_MESSAGE [-p PRIMARY_KEY]",
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
			{
				Comment: "commit all branches that have branch.file configured",
				Line:    "wrgl commit --all \"mass commit\"",
			},
			{
				Comment: "commit all branches using a transaction id (run 'wrgl transaction -h' to learn more about transaction)",
				Line:    "wrgl commit --all --txid a1dbfcc4-f6da-454c-a783-f1b70d347baf \"mass commit with transaction\"",
			},
		}),
		Args: cobra.MaximumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, cleanup, err := utils.SetupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
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
			all, err := cmd.Flags().GetBool("all")
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
			if err := utils.EnsureUserSet(cmd, c); err != nil {
				return err
			}
			branchName, csvFilePath, message, primaryKey, delim, commitFromBranchFile, err := parseCommitArgs(cmd, c, setFile, all, args)
			if err != nil {
				return err
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()

			tid, err := parseTxidFlag(cmd)
			if err != nil {
				return err
			}
			if tid != nil {
				cmd.Printf("With transaction %s\n", tid.String())
			}

			if all {
				return commitAllBranches(cmd, db, rs, c, message, numWorkers, memLimit, false, tid)
			}

			var sum []byte
			if commitFromBranchFile {
				sum, err = commitIfBranchFileHasChanged(cmd, db, rs, c, branchName, csvFilePath, primaryKey, message, numWorkers, memLimit, false, tid, delim)
				if err != nil {
					return err
				}
				if sum == nil {
					cmd.Printf("file %s hasn't changed since the last commit. Aborting.\n", csvFilePath)
					return nil
				}
			} else {
				sum, err = commit(cmd, db, rs, csvFilePath, message, branchName, primaryKey, numWorkers, memLimit, c, logger, false, tid, delim)
				if err != nil {
					return err
				}
			}
			cmd.Printf("[%s %s] %s\n", branchName, hex.EncodeToString(sum)[:7], message)

			return setBranchFile(rd, setFile, setPK, branchName, csvFilePath, primaryKey, delim)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize")
	cmd.Flags().Uint64("mem-limit", 0, "limit memory consumption (in bytes). If not set then memory limit is automatically calculated.")
	cmd.Flags().Bool("set-file", false, "set branch.file to CSV_FILE_PATH. If branch.file is set then you don't need to specify CSV_FILE_PATH in subsequent commits to BRANCH.")
	cmd.Flags().Bool("set-primary-key", false, "set branch.primaryKey to PRIMARY_KEY. If branch.primaryKey is set then you don't need to specify PRIMARY_KEY in subsequent commits to BRANCH.")
	cmd.Flags().Bool("all", false, "commit all branches that have branch.file configured.")
	cmd.Flags().String("txid", "", "commit using specified transaction id")
	cmd.Flags().String("delimiter", "", "CSV delimiter, defaults to comma")
	return cmd
}

func parseTxidFlag(cmd *cobra.Command) (tid *uuid.UUID, err error) {
	txid, err := cmd.Flags().GetString("txid")
	if err != nil {
		return nil, err
	}
	if txid != "" {
		tid = &uuid.UUID{}
		*tid, err = uuid.Parse(txid)
		if err != nil {
			return nil, fmt.Errorf("error parsing txid: %v", err)
		}
	}
	return tid, nil
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

func commit(
	cmd *cobra.Command, db objects.Store, rs ref.Store, csvFilePath, message, branchName string, primaryKey []string,
	numWorkers int, memLimit uint64, c *conf.Config, logger *logr.Logger, quiet bool, tid *uuid.UUID, delim rune,
) ([]byte, error) {
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

	var sortPT, blkPT pbar.Bar
	if !quiet {
		sortPT, blkPT = displayCommitProgress(cmd)
	}
	s, err := sorter.NewSorter(
		sorter.WithRunSize(memLimit),
		sorter.WithProgressBar(sortPT),
		sorter.WithDelimiter(delim),
	)
	if err != nil {
		return nil, err
	}
	sum, err := ingest.IngestTable(db, s, f, primaryKey,
		ingest.WithNumWorkers(numWorkers),
		ingest.WithProgressBar(blkPT),
		ingest.WithDebugLogger(logger),
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
	if err = saveHead(rs, branchName, commitSum, commit, tid); err != nil {
		return nil, err
	}
	return commitSum, nil
}

func commitTempBranch(
	cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, tmpBranch, csvFilePath string,
	primaryKey []string, numWorkers int, memLimit uint64, quiet bool, delim rune,
) (sum []byte, err error) {
	ref.DeleteHead(rs, tmpBranch)
	return commit(cmd, db, rs, csvFilePath, filepath.Base(csvFilePath), tmpBranch, primaryKey, numWorkers, memLimit, c, nil, quiet, nil, delim)
}

func getCommitTable(db objects.Store, rs ref.Store, branch string) (com *objects.Commit, tbl *objects.Table, err error) {
	sum, err := ref.GetHead(rs, branch)
	if err != nil {
		return
	}
	com, err = objects.GetCommit(db, sum)
	if err != nil {
		return
	}
	tbl, err = objects.GetTable(db, com.Table)
	return
}

func ensureTempCommit(
	cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, branch string, csvFilePath string,
	primaryKey []string, numWorkers int, memLimit uint64, quiet bool, delim rune,
) (sum []byte, err error) {
	tmpBranch := branch + "-tmp"
	com, tbl, err := getCommitTable(db, rs, tmpBranch)
	if err != nil {
		if errors.Is(err, objects.ErrKeyNotFound) || errors.Is(err, ref.ErrKeyNotFound) || errors.Is(err, io.ErrUnexpectedEOF) {
			sum, err = commitTempBranch(cmd, db, rs, c, tmpBranch, csvFilePath, primaryKey, numWorkers, memLimit, quiet, delim)
			if err != nil {
				return nil, err
			}
			return sum, nil
		}
		return nil, err
	}
	fd, err := os.Stat(csvFilePath)
	if err != nil {
		return nil, err
	}
	if com.Message != fd.Name() || com.Time.Before(fd.ModTime()) || !slice.StringSliceEqual(tbl.PrimaryKey(), primaryKey) {
		sum, err = commitTempBranch(cmd, db, rs, c, tmpBranch, csvFilePath, primaryKey, numWorkers, memLimit, quiet, delim)
		if err != nil {
			return nil, err
		}
		return sum, nil
	}
	return com.Sum, nil
}

func commitWithTable(cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, branch string, tableSum []byte, message string, tid *uuid.UUID) ([]byte, error) {
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
	if err = saveHead(rs, branch, commitSum, commit, tid); err != nil {
		return nil, err
	}
	return commitSum, nil
}

func saveHead(rs ref.Store, branch string, commitSum []byte, commit *objects.Commit, tid *uuid.UUID) error {
	if tid != nil {
		return ref.SaveTransactionRef(rs, *tid, branch, commitSum)
	}
	return ref.CommitHead(rs, branch, commitSum, commit, nil)
}

func commitIfBranchFileHasChanged(
	cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, branch string, csvFilePath string,
	primaryKey []string, message string, numWorkers int, memLimit uint64, quiet bool, tid *uuid.UUID, delim rune,
) ([]byte, error) {
	tmpSum, err := ensureTempCommit(cmd, db, rs, c, branch, csvFilePath, primaryKey, numWorkers, memLimit, quiet, delim)
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
	return commitWithTable(cmd, c, db, rs, branch, tmpCom.Table, message, tid)
}

func commitAllBranches(
	cmd *cobra.Command, db objects.Store, rs ref.Store, c *conf.Config, message string, numWorkers int, memLimit uint64, quiet bool, tid *uuid.UUID,
) error {
	for name, branch := range c.Branch {
		if tid != nil && strings.TrimPrefix(branch.Merge, "refs/heads/") != name {
			return fmt.Errorf("branch merge %q must end with %q", branch.Merge, name)
		}
		if branch.File == "" {
			continue
		}
		if _, err := os.Stat(branch.File); os.IsNotExist(err) {
			cmd.Printf("File %q does not exist, skipping branch %q.\n", branch.File, name)
			continue
		}
		sum, err := commitIfBranchFileHasChanged(cmd, db, rs, c, name, branch.File, branch.PrimaryKey, message, numWorkers, memLimit, quiet, tid, branch.Delimiter)
		if err != nil {
			return fmt.Errorf("error committing to branch %q: %v", name, err)
		}
		if sum == nil {
			cmd.Printf("branch %q is up-to-date.\n", name)
		} else {
			cmd.Printf("[%s %s] %s\n", name, hex.EncodeToString(sum)[:7], message)
		}
	}
	return nil
}

func setBranchFile(rd *local.RepoDir, setFile, setPK bool, branchName, csvFilePath string, primaryKey []string, delim rune) error {
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
			if delim != 0 {
				c.Branch[branchName].Delimiter = delim
			}
		}
		if setPK {
			c.Branch[branchName].PrimaryKey = primaryKey
		}
		return s.Save(c)
	}
	return nil
}

func parseCommitArgs(cmd *cobra.Command, c *conf.Config, setFile, all bool, args []string) (
	branchName, csvFilePath, message string, primaryKey []string, delim rune, commitFromBranchFile bool, err error,
) {
	primaryKey, err = cmd.Flags().GetStringSlice("primary-key")
	if err != nil {
		return
	}
	delim, err = utils.GetRuneFromFlag(cmd, "delimiter")
	if err != nil {
		return
	}
	if len(args) == 2 {
		branchName = args[0]
		message = args[1]
		errFileNotSet := fmt.Errorf("branch.file is not set for branch %q. You need to specify CSV_FILE_PATH", branchName)
		if c.Branch == nil {
			err = errFileNotSet
			return
		}
		if branch, ok := c.Branch[branchName]; !ok {
			err = errFileNotSet
			return
		} else if branch.File == "" {
			err = errFileNotSet
			return
		} else {
			csvFilePath = branch.File
			if len(primaryKey) == 0 && branch.PrimaryKey != nil {
				primaryKey = branch.PrimaryKey
			}
			delim = branch.Delimiter
			commitFromBranchFile = true
		}
	} else if len(args) == 3 {
		branchName = args[0]
		csvFilePath = args[1]
		message = args[2]
		if setFile && csvFilePath == "-" {
			err = fmt.Errorf("can't set branch.file while commiting from stdin")
			return
		}
	} else if all && len(args) == 1 {
		message = args[0]
	} else {
		cmd.Usage()
		err = fmt.Errorf("invalid number of arguments")
		return
	}
	return
}
