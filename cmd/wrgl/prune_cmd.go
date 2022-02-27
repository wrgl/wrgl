// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"io"
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune all unreachable objects from the object database.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
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
			allBlockIdxKeys, err := objects.GetAllBlockIndexKeys(db)
			if err != nil {
				return err
			}
			keepBlock := make([]bool, len(allBlockKeys))
			keepBlockIdx := make([]bool, len(allBlockIdxKeys))

			// remove orphaned tables
			err = pruneTables(cmd, db, survivingCommits, allBlockKeys, allBlockIdxKeys, keepBlock, keepBlockIdx)
			if err != nil {
				return err
			}

			// remove orphaned blocks
			bar := utils.PBar(-1, "removing blocks", cmd.OutOrStdout(), cmd.ErrOrStderr())
			for i, sum := range allBlockKeys {
				if !keepBlock[i] {
					if err := objects.DeleteBlock(db, sum); err != nil {
						return err
					}
					bar.Add(1)
				}
			}
			if err := bar.Finish(); err != nil {
				return err
			}
			bar = utils.PBar(-1, "removing block indices", cmd.OutOrStdout(), cmd.ErrOrStderr())
			for i, sum := range allBlockIdxKeys {
				if !keepBlockIdx[i] {
					if err := objects.DeleteBlockIndex(db, sum); err != nil {
						return err
					}
					bar.Add(1)
				}
			}
			if err := bar.Finish(); err != nil {
				return err
			}

			// remove orphaned commits
			bar = utils.PBar(-1, "removing commits", cmd.OutOrStdout(), cmd.ErrOrStderr())
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

func findCommitsToRemove(cmd *cobra.Command, db objects.Store, rs ref.Store) (commitsToRemove [][]byte, survivingCommits [][]byte, err error) {
	bar := utils.PBar(-1, "finding commits to remove", cmd.OutOrStdout(), cmd.ErrOrStderr())
	defer bar.Finish()
	refMap, err := ref.ListAllRefs(rs)
	if err != nil {
		return
	}
	q, err := ref.NewCommitsQueue(db, nil)
	if err != nil {
		return
	}
	for _, sum := range refMap {
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

func pruneTables(cmd *cobra.Command, db objects.Store, survivingCommits [][]byte, allBlockKeys, allBlockIdxKeys [][]byte, keepBlock, keepBlockIndex []bool) (err error) {
	bar := utils.PBar(-1, "removing small tables", cmd.OutOrStdout(), cmd.ErrOrStderr())
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
			if err := objects.DeleteTable(db, sum); err != nil {
				return err
			}
			if err := objects.DeleteTableIndex(db, sum); err != nil {
				return err
			}
			if err := objects.DeleteTableProfile(db, sum); err != nil {
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
				keepBlock[j] = true
			}
			for _, blk := range ts.BlockIndices {
				j := sort.Search(len(allBlockIdxKeys), func(i int) bool {
					return string(allBlockIdxKeys[i]) >= string(blk)
				})
				keepBlockIndex[j] = true
			}
		}
	}
	return nil
}
