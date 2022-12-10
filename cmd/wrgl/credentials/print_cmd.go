// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func getRemoteURI(arg string, c *conf.Config) (uriS string, uri *url.URL, err error) {
	uriS = arg
	if v, ok := c.Remote[uriS]; ok {
		uriS = v.URL
	}
	uriS = strings.TrimRight(uriS, "/")
	uri, err = url.Parse(uriS)
	return
}

func printCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print { REMOTE_URI | REMOTE_NAME }",
		Short: "Print access token for a remote, login if necessary",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cfs := conffs.NewStore(dir, conffs.LocalSource, "")
			c, err := cfs.Open()
			if err != nil {
				return fmt.Errorf("cfs.Open err: %w", err)
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return fmt.Errorf("credentials.NewStore err: %w", err)
			}

			uriS, uri, err := getRemoteURI(args[0], c)
			if err != nil {
				return fmt.Errorf("getRemoteURI err: %w", err)
			}
			if tok := cs.GetRPT(*uri); tok != "" {
				cmd.Println(tok)
			}

			logger := utils.GetLogger(cmd)
			cm := utils.NewClientMap(cs, *logger)
			client, err := cm.GetClient(cmd, uriS,
				apiclient.WithForceAuthenticate(),
			)
			if err != nil {
				return fmt.Errorf("cm.GetClient err: %w", err)
			}
			if _, err = client.GetRefs(nil, nil); err != nil {
				return fmt.Errorf("client.GetRefs err: %w", err)
			}
			return nil
		},
	}
	return cmd
}
