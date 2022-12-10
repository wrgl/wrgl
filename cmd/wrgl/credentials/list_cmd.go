// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List saved credentials by URI prefix.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := credentials.NewStore()
			if err != nil {
				return err
			}
			uris, err := s.RepoURIs()
			if err != nil {
				return err
			}
			for _, u := range uris {
				cmd.Println(u.String())
			}
			return nil
		},
	}
	return cmd
}
