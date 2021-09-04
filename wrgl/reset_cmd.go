// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset BRANCH COMMIT",
		Short: "Reset branch head commit to the specified commit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			branch := args[0]
			rd := getRepoDir(cmd)
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			quitIfRepoDirNotExist(cmd, rd)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			_, hash, _, err := ref.InterpretCommitName(db, rs, args[1], false)
			if err != nil {
				return err
			}
			if len(hash) == 0 {
				return fmt.Errorf("commit \"%s\" not found", args[1])
			}
			return ref.SaveRef(rs, "heads/"+branch, hash, c.User.Name, c.User.Email, "reset", "to commit "+hex.EncodeToString(hash))
		},
	}
	return cmd
}
