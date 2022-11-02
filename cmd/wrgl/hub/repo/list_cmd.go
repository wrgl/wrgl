// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package repo

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func getWrglHubCreds(cmd *cobra.Command) (cs *credentials.Store, uri *url.URL, token string, err error) {
	cs, err = credentials.NewStore()
	if err != nil {
		return
	}
	u, err := url.Parse(api.APIRoot)
	if err != nil {
		return
	}
	uri, token = cs.GetTokenMatching(*u)
	if token == "" {
		err = utils.CmdAuthError(cmd, u.String(), fmt.Errorf("Unauthenticated"))
		return
	}
	return
}

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [USERNAME]",
		Short: "list your repositories",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "list your own repositories",
				Line:    "wrgl hub repo list",
			},
			{
				Comment: "list repositories of another user",
				Line:    "wrgl hub repo list ipno",
			},
		}),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, uri, tok, err := getWrglHubCreds(cmd)
			if err != nil {
				return err
			}
			var username string
			if len(args) == 1 {
				username = args[0]
			} else {
				user, err := api.GetMe(tok)
				if err != nil {
					return utils.HandleHTTPError(cmd, cs, api.APIRoot, uri, err)
				}
				username = user.Username
			}
			cmd.Printf("Listing repositories of user %q\n", username)
			offset := 0
			for {
				lr, err := api.ListRepos(tok, username, offset)
				if err != nil {
					return utils.HandleHTTPError(cmd, cs, api.APIRoot, uri, err)
				}
				for _, obj := range lr.Repos {
					if obj.Public {
						cmd.Printf("  %s (public)\n", obj.Name)
					} else {
						cmd.Printf("  %s\n", obj.Name)
					}
				}
				offset += len(lr.Repos)
				if offset >= lr.Count {
					break
				}
			}
			return nil
		},
	}
	return cmd
}
