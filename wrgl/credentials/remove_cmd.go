package credentials

import (
	"net/url"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/credentials"
)

func removeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove URI...",
		Short: "Remove credentials matching URIs",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := credentials.NewStore()
			if err != nil {
				return err
			}
			for _, v := range args {
				u, err := url.Parse(v)
				if err != nil {
					return err
				}
				if ok := s.Delete(*u); ok {
					cmd.Printf("Removed credentials for %s\n", v)
				}
			}
			err = s.Flush()
			if err != nil {
				return err
			}
			cmd.Printf("Saved changes to %s\n", s.Path())
			return nil
		},
	}
	return cmd
}
