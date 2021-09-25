// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	"github.com/wrgl/core/pkg/conf"
	conffs "github.com/wrgl/core/pkg/conf/fs"
)

func addCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add NAME URL",
		Short: "Add a remote named NAME for the repository at URL.",
		Long:  "The command wrgl fetch NAME can then be used to create and update remote-tracking branches NAME/BRANCH.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			u := strings.TrimSuffix(args[1], "/")
			_, err := url.ParseRequestURI(u)
			if err != nil {
				return err
			}
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			tags, err := cmd.Flags().GetBool("tags")
			if err != nil {
				return err
			}
			track, err := cmd.Flags().GetStringSlice("track")
			if err != nil {
				return err
			}
			mirror, err := cmd.Flags().GetString("mirror")
			if err != nil {
				return err
			}
			if c.Remote == nil {
				c.Remote = map[string]*conf.Remote{}
			}
			c.Remote[name] = &conf.Remote{
				URL: u,
			}
			remote := c.Remote[name]
			if mirror == "fetch" {
				remote.Fetch = append(remote.Fetch, conf.MustParseRefspec("+refs/*:refs/*"))
			} else {
				if len(track) != 0 {
					for _, t := range track {
						remote.Fetch = append(remote.Fetch, conf.MustParseRefspec(
							fmt.Sprintf("+refs/heads/%s:refs/remotes/%s/%s", t, name, t),
						))
					}
				} else {
					remote.Fetch = append(remote.Fetch, conf.MustParseRefspec(
						fmt.Sprintf("+refs/heads/*:refs/remotes/%s/*", name),
					))
				}
				if tags {
					remote.Fetch = append(remote.Fetch, conf.MustParseRefspec("tag *"))
				}
			}
			if mirror == "push" {
				remote.Mirror = true
			}
			return s.Save(c)
		},
	}
	cmd.Flags().Bool("tags", false, "wrgl fetch NAME imports every tag from the remote repository")
	cmd.Flags().StringSliceP("track", "t", nil, strings.Join([]string{
		"With -t BRANCH, instead of the default glob refspec for the remote",
		"to track all branches under the refs/remote/NAME/ namespace, a refspec",
		"to track only BRANCH is created. You can give more than one -t BRANCH",
		"to rack multiple branches without grabbing all branches.",
	}, " "))
	cmd.Flags().String("mirror", "", strings.Join([]string{
		"With --mirror=fetch, the refs will not be stored in the refs/remotes/",
		"namespace, but rather everything in refs/ on the remote will be directly",
		"mirrored into /refs in the local repository.\nWith --mirror=push, wrgl",
		"push will always behave as if --mirror was passed.",
	}, " "))
	return cmd
}
