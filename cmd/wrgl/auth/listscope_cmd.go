// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func listscopeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-scope { EMAIL | anyone }",
		Short: "List scopes for a user.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "list scopes for a user",
				Line:    "wrgl auth list-scope john.doe@domain.com",
			},
			{
				Comment: "list scopes for anonymous users",
				Line:    "wrgl auth list-scope anyone",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if args[0] != auth.Anyone {
				authnS, err := authfs.NewAuthnStore(rd, c.TokenDuration())
				if err != nil {
					return err
				}
				defer authnS.Close()
				if !authnS.Exist(args[0]) {
					return UserNotFoundErr(args[0])
				}
			}
			authzS, err := authfs.NewAuthzStore(rd)
			if err != nil {
				return err
			}
			scopes, err := authzS.ListPolicies(args[0])
			if err != nil {
				return err
			}
			for _, scope := range scopes {
				cmd.Println(scope)
			}
			return nil
		},
	}
	return cmd
}
