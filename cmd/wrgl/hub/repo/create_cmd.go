// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package repo

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create NAME [ --set-remote REMOTE ]",
		Short: "Creates a new repository on WrglHub.",
		Args:  cobra.ExactArgs(1),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "create a new repository",
				Line:    "wrgl hub repo create my-new-repo",
			},
			{
				Comment: "create a new repository for an organization (that you belong to)",
				Line:    "wrgl hub repo create my-repo --user ipno",
			},
			{
				Comment: "create a new repo then set it origin remote for a local repository",
				Line:    "wrgl hub repo create my-repo --set-remote origin",
			},
		}),
		RunE: func(cmd *cobra.Command, args []string) error {
			setRemote, err := cmd.Flags().GetString("set-remote")
			if err != nil {
				return err
			}
			public, err := cmd.Flags().GetBool("public")
			if err != nil {
				return err
			}
			cs, tok, err := getWrglHubCreds(cmd)
			if err != nil {
				return err
			}
			username, err := cmd.Flags().GetString("user")
			if err != nil {
				return err
			}
			if username == "" {
				user, err := api.GetMe(tok)
				if err != nil {
					return utils.HandleHTTPError(cmd, cs, api.APIRoot, err)
				}
				username = user.Username
			}
			_, err = api.CreateRepo(tok, username, &api.CreateRepoRequest{
				Name:   args[0],
				Public: &public,
			})
			if err != nil {
				return err
			}
			cmd.Printf("Repository %q created at https://hub.wrgl.co/@%s/r/%s/\n", args[0], username, args[0])
			if setRemote != "" {
				return utils.AddRemote(cmd, setRemote, fmt.Sprintf("%s/users/%s/repos/%s", api.APIRoot, username, args[0]))
			}
			return nil
		},
	}
	cmd.Flags().Bool("public", false, "makes the repository public")
	cmd.Flags().StringP("user", "u", "", "instead of creating a repo under your username, create a repo for an organization (that you belong to) with this username")
	cmd.Flags().String("set-remote", "", "set the newly created repository as a remote with the specified name.")
	cmd.Flags().Bool("tags", false, "wrgl fetch REMOTE imports every tag from the remote repository")
	cmd.Flags().StringSliceP("track", "t", nil, strings.Join([]string{
		"with -t BRANCH, instead of tracking all remote branches, only track the",
		"specified BRANCH. You can give more than one -t BRANCH to track multiple",
		"branches while ignoring all other branches.",
	}, " "))
	cmd.Flags().String("mirror", "", strings.Join([]string{
		"with --mirror=fetch, the refs will not be stored in the refs/remotes/",
		"namespace, instead all references on the remote will be directly",
		"mirrored in the local repository.\nWith --mirror=push, wrgl push will",
		"always behave as if --mirror was passed.",
	}, " "))
	return cmd
}
