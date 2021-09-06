package auth

import "github.com/spf13/cobra"

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage authentication and authorization for wrgld",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.AddCommand(adduserCmd())
	cmd.AddCommand(listuserCmd())
	cmd.AddCommand(removeuserCmd())
	cmd.AddCommand(addscopeCmd())
	cmd.AddCommand(listscopeCmd())
	cmd.AddCommand(removescopeCmd())
	return cmd
}
