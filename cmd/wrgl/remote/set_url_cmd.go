// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	conffs "github.com/wrgl/core/pkg/conf/fs"
)

func setURLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-url NAME URL",
		Short: "Set URL for the remote",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			u := strings.TrimSuffix(args[1], "/")
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
