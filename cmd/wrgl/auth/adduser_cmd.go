package auth

import (
	"fmt"
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func adduserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add-user EMAIL",
		Short: "Add a user and set their name/password.",
		Long:  "Add a user and set their name/password. Once registered, users can log-in with their email/password and receive an access token via the Wrgld HTTP API. Look at the `credentials` command suite to find out more. If a user with the given email already exist then this command updates that user's info instead.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			email := args[0]
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			authnS, err := authfs.NewAuthnStore(rd, c.TokenDuration())
			if err != nil {
				return err
			}
			dw, cleanup, err := utils.SetupDebug(cmd)
			if err != nil {
				return err
			}
			if cleanup != nil {
				defer cleanup()
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
			if dw != nil {
				fmt.Fprintf(dw, "authn store internal state:\n%s\n", authnS.InternalState())
			}
			if err := authnS.Flush(); err != nil {
				return err
			}
			if dw != nil {
				b, err := ioutil.ReadFile(authnS.Filepath())
				if err != nil {
					fmt.Fprintf(dw, "error opening authn.csv: %v\n", err)
				}
				fmt.Fprintf(dw, "content from %s:\n%s\n", authnS.Filepath(), string(b))
			}
			return nil
		},
	}
	return cmd
}
