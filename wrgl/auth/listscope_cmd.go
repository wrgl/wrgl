package auth

import (
	"github.com/spf13/cobra"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func listscopeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "listscope EMAIL",
		Short: "List scopes for a user.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			authnS, err := authfs.NewAuthnStore(dir, c.TokenDuration())
			if err != nil {
				return err
			}
			if !authnS.Exist(args[0]) {
				return UserNotFoundErr(args[0])
			}
			authzS, err := authfs.NewAuthzStore(dir)
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
