// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/ref"
)

func renameCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename OLD NEW",
		Short: "Rename a remote.",
		Long:  "Rename a remote. All remote-tracking branches and configuration settings for the remote are updated.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldRem := args[0]
			newRem := args[1]
			wrglDir := utils.MustWRGLDir(cmd)
			if oldRem == newRem {
				return nil
			}
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			utils.MustGetRemote(cmd, c, oldRem)
			rd := utils.GetRepoDir(cmd)
			rs := rd.OpenRefStore()
			err = ref.RenameAllRemoteRefs(rs, oldRem, newRem)
			if err != nil {
				return err
			}
			c.Remote[newRem] = c.Remote[oldRem]
			delete(c.Remote, oldRem)
			return s.Save(c)
		},
	}
	return cmd
}
