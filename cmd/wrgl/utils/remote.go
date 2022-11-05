// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"net/url"
	"os"
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func MustGetRemote(cmd *cobra.Command, c *conf.Config, name string) *conf.Remote {
	v, ok := c.Remote[name]
	if !ok {
		cmd.PrintErrf("fatal: No such remote '%s'\n", name)
		os.Exit(1)
	}
	return v
}

func AddRemote(cmd *cobra.Command, name string, uri string) error {
	wrglDir := MustWRGLDir(cmd)
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
		URL: uri,
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
}

func GetCredentials(cmd *cobra.Command, cs *credentials.Store, remote string) (token string, err error) {
	u, err := url.Parse(remote)
	if err != nil {
		return
	}
	token = cs.GetTokenMatching(*u)
	return
}
