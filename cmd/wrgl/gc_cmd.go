// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func gcCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Garbage collect expired transactions and loose objects",
		Long:  "Garbage collect expired transactions and loose objects. This command runs `prune` command as one of its subroutines so you don't have to run `prune` separately.",
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
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer *pbar.Container) error {
				bar := barContainer.NewBar(-1, "Discarding expired transactions", 0)
				defer bar.Done()
				return transaction.GarbageCollect(
					db, rs, c.GetTransactionTTL(),
					bar,
				)
			}); err != nil {
				return err
			}
			return runPrune(cmd, db, rs)
		},
	}
	return cmd
}
