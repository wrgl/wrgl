// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package prune

import (
	"io"
	"sort"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func findCommitsToRemove(db objects.Store, rs ref.Store, pbarAdd func()) (commitsToRemove [][]byte, survivingCommits [][]byte, err error) {
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
			pbarAdd()
		} else {
			survivingCommits = append(survivingCommits, commitKeys[i])
		}
	}
	return
}

func pruneTables(db objects.Store, survivingCommits [][]byte, allBlockKeys, allBlockIdxKeys [][]byte, keepBlock, keepBlockIndex []bool) runProgressFunc {
	return func(pbarAdd func()) (err error) {
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
				pbarAdd()
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
}

type PruneOptions struct {
	FindCommitsPbar       func() *progressbar.ProgressBar
	PruneTablesPbar       func() *progressbar.ProgressBar
	PruneBlocksPbar       func() *progressbar.ProgressBar
	PruneBlockIndicesPbar func() *progressbar.ProgressBar
	PruneCommitsPbar      func() *progressbar.ProgressBar
}

type runProgressFunc func(pbarAdd func()) (err error)

func runWithPbar(getPbar func() *progressbar.ProgressBar, run runProgressFunc) error {
	var pbar *progressbar.ProgressBar
	if getPbar != nil {
		pbar = getPbar()
		defer pbar.Finish()
	}
	return run(func() {
		if pbar != nil {
			pbar.Add(1)
		}
	})
}

func Prune(db objects.Store, rs ref.Store, opts *PruneOptions) (err error) {
	if opts == nil {
		opts = &PruneOptions{}
	}
	var commitsToRemove, survivingCommits [][]byte
	if err = runWithPbar(opts.FindCommitsPbar, func(pbarAdd func()) (err error) {
		commitsToRemove, survivingCommits, err = findCommitsToRemove(db, rs, pbarAdd)
		return err
	}); err != nil {
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
	if err = runWithPbar(opts.PruneTablesPbar, pruneTables(db, survivingCommits, allBlockKeys, allBlockIdxKeys, keepBlock, keepBlockIdx)); err != nil {
		return err
	}

	// remove orphaned blocks
	if err = runWithPbar(opts.PruneBlocksPbar, func(pbarAdd func()) (err error) {
		for i, sum := range allBlockKeys {
			if !keepBlock[i] {
				if err := objects.DeleteBlock(db, sum); err != nil {
					return err
				}
				pbarAdd()
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err = runWithPbar(opts.PruneBlockIndicesPbar, func(pbarAdd func()) (err error) {
		for i, sum := range allBlockIdxKeys {
			if !keepBlockIdx[i] {
				if err := objects.DeleteBlockIndex(db, sum); err != nil {
					return err
				}
				pbarAdd()
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// remove orphaned commits
	return runWithPbar(opts.PruneCommitsPbar, func(pbarAdd func()) (err error) {
		for _, sum := range commitsToRemove {
			err = objects.DeleteCommit(db, sum)
			if err != nil {
				return err
			}
			pbarAdd()
		}
		return nil
	})
}
