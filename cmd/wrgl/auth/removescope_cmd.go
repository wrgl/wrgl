package auth

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func UserNotFoundErr(email string) error {
	return fmt.Errorf("cannot find user with email %q, make sure to add user with `wrgl auth adduser` first", email)
}

func InvalidScopeErr(scope string) error {
	return fmt.Errorf("invalid scope: %q, valid scopes are:\n%s", scope, allScopesString(2, false))
}

func removescopeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-scope { EMAIL | anyone } SCOPE...",
		Short: "Remove one or more scopes for a user.",
		Long:  "Remove one or more scopes for a user. Scope represents what actions are allowed via the Wrgld HTTP API for a users. Valid scopes are:\n" + allScopesString(2, true),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "remove write scopes for a user",
				Line:    fmt.Sprintf("wrgl auth remove-scope john.doe@domain.com %s %s", auth.ScopeRepoWrite, auth.ScopeRepoWriteConfig),
			},
			{
				Comment: "remove public access",
				Line:    fmt.Sprintf("wrgl auth remove-scope %s %s", auth.Anyone, auth.ScopeRepoRead),
			},
		}),
		Args: cobra.MinimumNArgs(2),
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
			for _, scope := range args[1:] {
				if _, ok := validScopes[scope]; !ok {
					return InvalidScopeErr(scope)
				}
				if err := authzS.RemovePolicy(args[0], scope); err != nil {
					return err
				}
			}
			return authzS.Flush()
		},
	}
	return cmd
}
