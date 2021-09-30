// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/ref"
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
