package credentials

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/credentials"
)

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List credentials by URI prefix",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := credentials.NewStore()
			if err != nil {
				return err
			}
			for _, u := range s.URIs() {
				cmd.Println(u.String())
			}
			return nil
		},
	}
	return cmd
}
