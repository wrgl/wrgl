// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/prune"
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

			return runPrune(cmd, db, rs)
		},
	}
	return cmd
}

func runPrune(cmd *cobra.Command, db objects.Store, rs ref.Store) error {
	return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer *pbar.Container) error {
		findCommitsBar := barContainer.NewBar(-1, "finding commits to remove", 0)
		pruneTablesBar := barContainer.NewBar(-1, "removing small tables", 0)
		pruneBlocksBar := barContainer.NewBar(-1, "removing blocks", 0)
		pruneBlockIndicesBar := barContainer.NewBar(-1, "removing block indices", 0)
		pruneCommitsBar := barContainer.NewBar(1, "removing commits", 0)
		return prune.Prune(db, rs, &prune.PruneOptions{
			FindCommitsPbar: func() pbar.Bar {
				return findCommitsBar
			},
			PruneTablesPbar: func() pbar.Bar {
				return pruneTablesBar
			},
			PruneBlocksPbar: func() pbar.Bar {
				return pruneBlocksBar
			},
			PruneBlockIndicesPbar: func() pbar.Bar {
				return pruneBlockIndicesBar
			},
			PruneCommitsPbar: func() pbar.Bar {
				return pruneCommitsBar
			},
		})
	})
}
