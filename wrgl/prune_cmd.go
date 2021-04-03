package main

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func newPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune all unreachable objects from the object database",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			fileStore := rd.OpenFileStore()

			commitsToRemove, survivingCommits, err := findCommitsToRemove(cmd, kvStore)
			if err != nil {
				return err
			}
			if len(commitsToRemove) == 0 {
				return nil
			}

			allRowKeys, err := table.GetAllRowKeys(kvStore)
			if err != nil {
				return err
			}
			keepRow := make([]bool, len(allRowKeys))

			// remove orphaned tables
			err = pruneSmallTables(cmd, kvStore, survivingCommits, allRowKeys, keepRow)
			if err != nil {
				return err
			}
			err = pruneBigTables(cmd, kvStore, fileStore, survivingCommits, allRowKeys, keepRow)
			if err != nil {
				return err
			}

			// remove orphaned rows
			bar := pbar(-1, "removing rows", cmd.OutOrStdout(), cmd.ErrOrStderr())
			for i, hash := range allRowKeys {
				if !keepRow[i] {
					err := table.DeleteRow(kvStore, []byte(hash))
					if err != nil {
						return err
					}
					bar.Add(1)
				}
			}
			if err := bar.Finish(); err != nil {
				return err
			}

			// remove orphaned commits
			bar = pbar(-1, "removing commits", cmd.OutOrStdout(), cmd.ErrOrStderr())
			for _, hash := range commitsToRemove {
				err = versioning.DeleteCommit(kvStore, hash)
				if err != nil {
					return err
				}
				bar.Add(1)
			}
			return bar.Finish()
		},
	}
	return cmd
}

func pbar(max int64, desc string, out, err io.Writer) *progressbar.ProgressBar {
	bar := progressbar.NewOptions64(
		max,
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetWriter(out),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(err, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	)
	bar.RenderBlank()
	return bar
}

func findCommitsToRemove(cmd *cobra.Command, kvStore kv.Store) (commitsToRemove []string, survivingCommits []string, err error) {
	bar := pbar(-1, "finding commits to remove", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	branchMap, err := versioning.ListBranch(kvStore)
	if err != nil {
		return nil, nil, err
	}
	commitHashes, err := versioning.GetAllCommitHashes(kvStore)
	if err != nil {
		return nil, nil, err
	}
	commitFound := make([]bool, len(commitHashes))
	for _, b := range branchMap {
		hash := b.CommitHash
		for hash != "" {
			ind := sort.SearchStrings(commitHashes, hash)
			commitFound[ind] = true
			commit, err := versioning.GetCommit(kvStore, hash)
			if err != nil {
				return nil, nil, err
			}
			hash = commit.PrevCommitHash
		}
	}
	for i, found := range commitFound {
		if !found {
			commitsToRemove = append(commitsToRemove, commitHashes[i])
			bar.Add(1)
		} else {
			survivingCommits = append(survivingCommits, commitHashes[i])
		}
	}
	return
}

func pruneSmallTables(cmd *cobra.Command, kvStore kv.Store, survivingCommits, allRowKeys []string, keepRow []bool) (err error) {
	bar := pbar(-1, "removing small tables", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	tableHashes, err := table.GetAllSmallTableHashes(kvStore)
	if err != nil {
		return
	}
	tableFound := make([]bool, len(tableHashes))
	for _, commitHash := range survivingCommits {
		commit, err := versioning.GetCommit(kvStore, commitHash)
		if err != nil {
			return err
		}
		if commit.TableStoreType != table.Small {
			continue
		}
		i := sort.SearchStrings(tableHashes, commit.ContentHash)
		tableFound[i] = true
	}
	for i, keep := range tableFound {
		hash := tableHashes[i]
		if !keep {
			err := table.DeleteSmallStore(kvStore, hash)
			if err != nil {
				return err
			}
			bar.Add(1)
		} else {
			ts, err := table.ReadSmallStore(kvStore, 0, hash)
			if err != nil {
				return err
			}
			reader, err := ts.NewRowHashReader(0, 0)
			if err != nil {
				return err
			}
			for {
				_, rowhash, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				j := sort.SearchStrings(allRowKeys, string(rowhash))
				keepRow[j] = true
			}
		}
	}
	return nil
}

func pruneBigTables(cmd *cobra.Command, kvStore kv.Store, fileStore kv.FileStore, survivingCommits, allRowKeys []string, keepRow []bool) (err error) {
	bar := pbar(-1, "removing big tables", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	tableHashes, err := table.GetAllBigTableHashes(kvStore)
	if err != nil {
		return
	}
	tableFound := make([]bool, len(tableHashes))
	for _, commitHash := range survivingCommits {
		commit, err := versioning.GetCommit(kvStore, commitHash)
		if err != nil {
			return err
		}
		if commit.TableStoreType != table.Big {
			continue
		}
		i := sort.SearchStrings(tableHashes, commit.ContentHash)
		tableFound[i] = true
	}
	for i, keep := range tableFound {
		hash := tableHashes[i]
		if !keep {
			err := table.DeleteBigStore(kvStore, fileStore, hash)
			if err != nil {
				return err
			}
			bar.Add(1)
		} else {
			ts, err := table.ReadBigStore(kvStore, fileStore, 0, hash)
			if err != nil {
				return err
			}
			reader, err := ts.NewRowHashReader(0, 0)
			if err != nil {
				return err
			}
			for {
				_, rowhash, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				j := sort.SearchStrings(allRowKeys, string(rowhash))
				keepRow[j] = true
			}
		}
	}
	return nil
}
