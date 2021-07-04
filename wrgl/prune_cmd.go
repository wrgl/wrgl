// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

func newPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune all unreachable objects from the object database",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()

			commitsToRemove, survivingCommits, err := findCommitsToRemove(cmd, db, rs)
			if err != nil {
				return err
			}
			if len(commitsToRemove) == 0 {
				return nil
			}

			allBlockKeys, err := objects.GetAllBlockKeys(db)
			if err != nil {
				return err
			}
			keepRow := make([]bool, len(allBlockKeys))

			// remove orphaned tables
			err = pruneTables(cmd, db, survivingCommits, allBlockKeys, keepRow)
			if err != nil {
				return err
			}

			// remove orphaned rows
			bar := pbar(-1, "removing rows", cmd.OutOrStdout(), cmd.ErrOrStderr())
			for i, sum := range allBlockKeys {
				if !keepRow[i] {
					err := objects.DeleteBlock(db, sum)
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
			for _, sum := range commitsToRemove {
				err = objects.DeleteCommit(db, sum)
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

func findCommitsToRemove(cmd *cobra.Command, db objects.Store, rs ref.Store) (commitsToRemove [][]byte, survivingCommits [][]byte, err error) {
	bar := pbar(-1, "finding commits to remove", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	branchMap, err := ref.ListHeads(rs)
	if err != nil {
		return
	}
	q, err := ref.NewCommitsQueue(db, nil)
	if err != nil {
		return
	}
	for _, sum := range branchMap {
		q.Insert(sum)
	}
	commitKeys, err := objects.GetAllCommitKeys(db)
	if err != nil {
		return nil, nil, err
	}
	commitFound := make([]bool, len(commitKeys))
	for {
		sum, _, err := q.PopInsertParents()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		ind := sort.Search(len(commitKeys), func(i int) bool {
			return string(commitKeys[i]) >= string(sum)
		})
		commitFound[ind] = true
	}
	for i, found := range commitFound {
		if !found {
			commitsToRemove = append(commitsToRemove, commitKeys[i])
			bar.Add(1)
		} else {
			survivingCommits = append(survivingCommits, commitKeys[i])
		}
	}
	return
}

func pruneTables(cmd *cobra.Command, db objects.Store, survivingCommits [][]byte, allBlockKeys [][]byte, keepRow []bool) (err error) {
	bar := pbar(-1, "removing small tables", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	tableHashes, err := objects.GetAllTableKeys(db)
	if err != nil {
		return
	}
	tableFound := make([]bool, len(tableHashes))
	for _, commitHash := range survivingCommits {
		commit, err := objects.GetCommit(db, commitHash)
		if err != nil {
			return err
		}
		i := sort.Search(len(tableHashes), func(i int) bool { return string(tableHashes[i]) >= string(commit.Table) })
		tableFound[i] = true
	}
	for i, keep := range tableFound {
		sum := tableHashes[i]
		if !keep {
			err := objects.DeleteTable(db, sum)
			if err != nil {
				return err
			}
			bar.Add(1)
		} else {
			ts, err := objects.GetTable(db, sum)
			if err != nil {
				return err
			}
			for _, blk := range ts.Blocks {
				j := sort.Search(len(allBlockKeys), func(i int) bool {
					return string(allBlockKeys[i]) >= string(blk)
				})
				keepRow[j] = true
			}
		}
	}
	return nil
}
