// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func removeuserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-user EMAIL...",
		Short: "Remove users with email.",
		Args:  cobra.MinimumNArgs(1),
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
			for _, email := range args {
				if err := authnS.RemoveUser(email); err != nil {
					return err
				}
			}
			return authnS.Flush()
		},
	}
	return cmd
}
