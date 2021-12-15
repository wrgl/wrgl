// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func addCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add REMOTE_NAME URL",
		Short: "Add a remote repository.",
		Long:  "Add a remote repository at the specified URL. Track remote branches with refspec +refs/heads/*:refs/remotes/REMOTE_NAME/* by default (save to configuration option remote.<remote>.fetch). You can then use the command \"wrgl fetch REMOTE_NAME\" to fetch remote refs.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "add a remote repository tracking all remote branches",
				Line:    "wrgl remote add origin https://my-remote-repository",
			},
			{
				Comment: "add a remote repository tracking only the branch main",
				Line:    "wrgl remote add origin https://my-remote-repository -t main",
			},
			{
				Comment: "add a remote repository, making the local repository a mirror of this remote",
				Line:    "wrgl remote add origin https://my-remote-repository --mirror=fetch",
			},
			{
				Comment: "add a remote repository, mirroring the local repository",
				Line:    "wrgl remote add origin https://my-remote-repository --mirror=push",
			},
		}),
		Args: cobra.ExactArgs(2),
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
			sort.Sort(remote.Fetch)
			if mirror == "push" {
				remote.Mirror = true
			}
			return s.Save(c)
		},
	}
	cmd.Flags().Bool("tags", false, "wrgl fetch NAME imports every tag from the remote repository")
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
