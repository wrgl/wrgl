package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
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
			bigTable, err := cmd.Flags().GetBool("big-table")
			if err != nil {
				return err
			}
			smallTable, err := cmd.Flags().GetBool("small-table")
			if err != nil {
				return err
			}
			return commit(cmd, csvFilePath, message, branchName, primaryKey, numWorkers, bigTable, smallTable)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key for table")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	cmd.Flags().Bool("small-table", false, "use small table store. This is the default table store.")
	cmd.Flags().Bool("big-table", false, "use big table store. Big table store is great when dealing with files that are a few GiB or more.")
	return cmd
}

func decideTableStoreType(db kv.DB, branch *versioning.Branch, bigTable, smallTable bool) (table.StoreType, error) {
	if bigTable {
		return table.Big, nil
	} else if smallTable {
		return table.Small, nil
	}
	if branch.CommitHash != "" {
		prevCommit, err := versioning.GetCommit(db, branch.CommitHash)
		if err != nil {
			return 0, err
		}
		return prevCommit.TableStoreType, nil
	}
	return table.Small, nil
}

func getRepoDir(cmd *cobra.Command) *repoDir {
	rootDir, err := cmd.Flags().GetString("root-dir")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
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
	rd := &repoDir{
		rootDir:        rootDir,
		badgerLogInfo:  badgerLogInfo,
		badgerLogDebug: badgerLogDebug,
	}
	return rd
}

func quitIfRepoDirNotExist(cmd *cobra.Command, rd *repoDir) {
	if !rd.Exist() {
		cmd.PrintErrf("Repository not initialized in directory \"%s\". Initialize with command:\n", rd.rootDir)
		cmd.PrintErrln("  wrgl init")
		os.Exit(1)
	}
}

func commit(cmd *cobra.Command, csvFilePath, message, branchName string, primaryKey []string, numWorkers int, bigTable, smallTable bool) error {
	if !versioning.BranchPattern.MatchString(branchName) {
		return fmt.Errorf("invalid repo name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	c, err := aggregateConfig(cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	rd := getRepoDir(cmd)
	quitIfRepoDirNotExist(cmd, rd)
	kvStore, err := rd.OpenKVStore()
	if err != nil {
		return err
	}
	defer kvStore.Close()

	// detect table store type
	branch, err := versioning.GetBranch(kvStore, branchName)
	if err != nil {
		branch = &versioning.Branch{}
	}
	tsType, err := decideTableStoreType(kvStore, branch, bigTable, smallTable)
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
	if tsType == table.Big {
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
		PrevCommitHash: branch.CommitHash,
		Timestamp:      time.Now(),
		Author: &versioning.Author{
			Email: c.User.Email,
			Name:  c.User.Name,
		},
	}
	if bigTable {
		commit.TableStoreType = table.Big
	}
	commitSum, err := commit.Save(kvStore, seed)
	if err != nil {
		return err
	}
	branch.CommitHash = commitSum
	err = branch.Save(kvStore, branchName)
	if err != nil {
		return err
	}
	cmd.Printf("[%s %s] %s\n", branchName, commitSum[:7], message)
	return nil
}
