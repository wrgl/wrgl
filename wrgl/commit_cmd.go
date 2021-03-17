package main

import (
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func newCommitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit CSV_FILE_PATH REPO_NAME MESSAGE",
		Short: "Commit CSV file to a repo",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			csvFilePath := args[0]
			reponame := args[1]
			message := args[2]
			rootDir, err := cmd.Flags().GetString("root-dir")
			if err != nil {
				return err
			}
			primaryKey, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			numWorkers, err := cmd.Flags().GetInt("num-workers")
			if err != nil {
				return err
			}
			badgerLogInfo, err := cmd.Flags().GetBool("badger-log-info")
			if err != nil {
				return err
			}
			badgerLogDebug, err := cmd.Flags().GetBool("badger-log-debug")
			if err != nil {
				return err
			}
			return commit(cmd, csvFilePath, message, rootDir, reponame, primaryKey, numWorkers, badgerLogDebug, badgerLogInfo)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	cmd.Flags().Bool("badger-log-info", false, "set Badger log level to INFO")
	cmd.Flags().Bool("badger-log-debug", false, "set Badger log level to DEBUG")
	return cmd
}

func commit(cmd *cobra.Command, csvFilePath, message, rootDir, reponame string, primaryKey []string, numWorkers int, badgerLogDebug, badgerLogInfo bool) error {
	var seed uint64 = 0
	c, err := aggregateConfig(cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	rd := &repoDir{
		rootDir: rootDir,
		name:    reponame,
	}
	if !rd.Exist() {
		cmd.PrintErrf("Repository with name \"%s\" does not exist. Create with this command:\n", reponame)
		cmd.PrintErrf("  wrgl init %s", reponame)
		os.Exit(1)
	}
	kvStore, err := rd.OpenKVStore(badgerLogDebug, badgerLogInfo)
	if err != nil {
		return err
	}
	repo, err := versioning.GetRepo(kvStore)
	if err != nil {
		return err
	}
	f, err := os.Open(csvFilePath)
	if err != nil {
		return err
	}
	defer f.Close()
	csvReader, columns, primaryKeyIndices, err := ingest.ReadColumns(f, primaryKey)
	if err != nil {
		return err
	}
	var ts table.Store
	if repo.TableStoreType == table.Big {
		fileStore := rd.OpenFileStore()
		ts, err = table.NewBigStore(kvStore, fileStore, columns, primaryKeyIndices, seed)
		if err != nil {
			return err
		}
	} else {
		ts = table.NewSmallStore(kvStore, columns, primaryKeyIndices, seed)
	}
	sum, err := ingest.Ingest(seed, numWorkers, csvReader, primaryKeyIndices, ts, cmd.OutOrStdout())
	if err != nil {
		return err
	}
	commit := &versioning.Commit{
		ContentHash:    sum,
		Message:        message,
		PrevCommitHash: repo.CommitHash,
		Timestamp:      time.Now(),
		Author: &versioning.Author{
			Email: c.User.Email,
			Name:  c.User.Name,
		},
	}
	commitSum, err := commit.Save(kvStore, seed)
	if err != nil {
		return err
	}
	repo.CommitHash = commitSum
	err = repo.Save(kvStore)
	if err != nil {
		return err
	}
	return nil
}
