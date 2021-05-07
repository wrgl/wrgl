package remote

import (
	"net/url"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
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
			c, err := versioning.OpenConfig(false, wrglDir)
			if err != nil {
				return err
			}
			rem := mustGetRemote(cmd, c, name)
			rem.URL = u
			return c.Save()
		},
	}
	return cmd
}
