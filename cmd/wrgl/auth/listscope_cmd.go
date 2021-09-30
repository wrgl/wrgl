package auth

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
)

func listscopeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-scope EMAIL",
		Short: "List scopes for a user.",
		Args:  cobra.ExactArgs(1),
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
			if !authnS.Exist(args[0]) {
				return UserNotFoundErr(args[0])
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
