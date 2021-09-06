package auth

import (
	"github.com/spf13/cobra"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func removeuserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "removeuser EMAIL...",
		Short: "Remove user with EMAIL",
		Args:  cobra.MinimumNArgs(1),
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
			for _, email := range args {
				if err := authnS.RemoveUser(email); err != nil {
					return err
				}
			}
			return authnS.Flush()
		},
	}
	return cmd
}
