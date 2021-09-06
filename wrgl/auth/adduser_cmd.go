package auth

import (
	"github.com/spf13/cobra"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func adduserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "adduser EMAIL",
		Short: "Add user and set their password.",
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
			password, err := utils.PromptForPassword(cmd)
			if err != nil {
				return err
			}
			if err := authnS.SetPassword(args[0], password); err != nil {
				return err
			}
			return authnS.Flush()
		},
	}
	return cmd
}
