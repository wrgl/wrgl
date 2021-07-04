// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"net/url"

	"github.com/spf13/cobra"
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
			c, err := utils.OpenConfig(false, false, wrglDir, "")
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			rem.URL = u
			return utils.SaveConfig(c)
		},
	}
	return cmd
}
