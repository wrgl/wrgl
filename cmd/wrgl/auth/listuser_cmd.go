// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func listuserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-user",
		Short: "List registered users.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			authnS, err := authfs.NewAuthnStore(rd, c.TokenDuration())
			if err != nil {
				return err
			}
			users, err := authnS.ListUsers()
			if err != nil {
				return err
			}
			sort.Slice(users, func(i, j int) bool {
				return users[i][0] < users[j][0]
			})
			rows := make([][]string, len(users))
			for _, sl := range users {
				email, name := sl[0], sl[1]
				rows = append(rows, []string{
					name, fmt.Sprintf("<%s>", email),
				})
			}
			utils.PrintTable(cmd.OutOrStdout(), rows, 0)
			return nil
		},
	}
	return cmd
}
