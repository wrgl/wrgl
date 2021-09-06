package auth

import (
	"sort"

	"github.com/spf13/cobra"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func listuserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "listuser",
		Short: "List registered users.",
		Args:  cobra.ExactArgs(0),
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
			emails, err := authnS.ListUsers()
			if err != nil {
				return err
			}
			sort.Strings(emails)
			for _, email := range emails {
				cmd.Println(email)
			}
			return nil
		},
	}
	return cmd
}
