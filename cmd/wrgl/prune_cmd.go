// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
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
		FindCommitsPbar: func() *progressbar.ProgressBar {
			return utils.PBar(-1, "finding commits to remove", cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		PruneTablesPbar: func() *progressbar.ProgressBar {
			return utils.PBar(-1, "removing small tables", cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		PruneBlocksPbar: func() *progressbar.ProgressBar {
			return utils.PBar(-1, "removing blocks", cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		PruneBlockIndicesPbar: func() *progressbar.ProgressBar {
			return utils.PBar(-1, "removing block indices", cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		PruneCommitsPbar: func() *progressbar.ProgressBar {
			return utils.PBar(-1, "removing commits", cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	})
}
