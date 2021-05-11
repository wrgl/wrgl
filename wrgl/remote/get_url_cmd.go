package remote

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func getURLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-url NAME",
		Short: "Retrieves the URLs for a remote.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := versioning.OpenConfig(false, wrglDir)
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			cmd.Println(rem.URL)
			return nil
		},
	}
	return cmd
}
