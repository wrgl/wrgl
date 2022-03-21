// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset BRANCH COMMIT",
		Short: "Reset branch ref to the specified commit.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "reset branch to the previous commit",
				Line:    "wrgl reset main main^",
			},
			{
				Comment: "reset branch to an arbitrary commit",
				Line:    "wrgl reset main 43a5f3447e82b53a2574ef5af470df96",
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			branch := args[0]
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			_, hash, com, err := ref.InterpretCommitName(db, rs, args[1], false)
			if err != nil {
				return err
			}
			if len(hash) == 0 {
				return fmt.Errorf("commit \"%s\" not found", args[1])
			}
			if !objects.TableExist(db, com.Table) {
				if remote, err := utils.FindRemoteFor(db, rs, hash); err != nil {
					return err
				} else if remote != "" {
					return fmt.Errorf("cannot reset branch to a shallow commit: table %x is missing. Fetch missing table with:\n  wrgl fetch tables %s %x", com.Table, remote, com.Table)
				}
				return fmt.Errorf("cannot reset branch to a shallow commit: table %x is missing", com.Table)
			}
			return ref.SaveRef(rs, "heads/"+branch, hash, c.User.Name, c.User.Email, "reset", "to commit "+hex.EncodeToString(hash), nil)
		},
	}
	return cmd
}
