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
	return prune.Prune(db, rs, &prune.PruneOptions{
		FindCommitsPbar: func() pbar.Bar {
			return pbar.NewProgressBar(cmd.OutOrStdout(), -1, "finding commits to remove")
		},
		PruneTablesPbar: func() pbar.Bar {
			return pbar.NewProgressBar(cmd.OutOrStdout(), -1, "removing small tables")
		},
		PruneBlocksPbar: func() pbar.Bar {
			return pbar.NewProgressBar(cmd.OutOrStdout(), -1, "removing blocks")
		},
		PruneBlockIndicesPbar: func() pbar.Bar {
			return pbar.NewProgressBar(cmd.OutOrStdout(), -1, "removing block indices")
		},
		PruneCommitsPbar: func() pbar.Bar {
			return pbar.NewProgressBar(cmd.OutOrStdout(), -1, "removing commits")
		},
	})
}
