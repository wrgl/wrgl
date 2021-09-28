// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/ref"
)

func removeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove NAME",
		Aliases: []string{"rm"},
		Short:   "Remove a remote.",
		Long:    "Remote a remote. All remote-tracking branches and configuration settings for the remote are removed. This won't remove the credentials for this remote however. To remove the credentials, use \"wrgl credentials remove\".",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rd := local.NewRepoDir(wrglDir, false, false)
			rs := rd.OpenRefStore()
			err = ref.DeleteAllRemoteRefs(rs, args[0])
			if err != nil {
				return err
			}
			delete(c.Remote, args[0])
			return s.Save(c)
		},
	}
	return cmd
}
