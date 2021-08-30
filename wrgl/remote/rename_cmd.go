// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func renameCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename OLD NEW",
		Short: "Rename the remote named OLD to NEW.",
		Long:  "All remote-tracking branches and configuration settings for the remote are updated",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldRem := args[0]
			newRem := args[1]
			wrglDir := utils.MustWRGLDir(cmd)
			if oldRem == newRem {
				return nil
			}
			c, err := local.OpenConfig(false, false, wrglDir, "")
			if err != nil {
				return err
			}
			utils.MustGetRemote(cmd, c, oldRem)
			rd := local.NewRepoDir(wrglDir, false, false)
			rs := rd.OpenRefStore()
			err = ref.RenameAllRemoteRefs(rs, oldRem, newRem)
			if err != nil {
				return err
			}
			c.Remote[newRem] = c.Remote[oldRem]
			delete(c.Remote, oldRem)
			return local.SaveConfig(c)
		},
	}
	return cmd
}
