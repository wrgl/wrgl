// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"net/url"

	"github.com/spf13/cobra"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func setURLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-url NAME URL",
		Short: "Changes URL for the remote",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			u := args[1]
			_, err := url.ParseRequestURI(u)
			if err != nil {
				return err
			}
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			rem.URL = u
			return s.Save(c)
		},
	}
	return cmd
}
