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
		Short: "Add user and set their name and password.",
		Long:  "Add user and set their name and password. If a user with this email already exist then update that user's info instead.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			email := args[0]
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
			name, err := utils.Prompt(cmd, "Name")
			if err != nil {
				return err
			}
			if err := authnS.SetName(email, name); err != nil {
				return err
			}
			password, err := utils.PromptForPassword(cmd)
			if err != nil {
				return err
			}
			if err := authnS.SetPassword(email, password); err != nil {
				return err
			}
			return authnS.Flush()
		},
	}
	return cmd
}
