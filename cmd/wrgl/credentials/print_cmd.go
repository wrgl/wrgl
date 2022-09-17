// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func printCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print { REMOTE_URI | REMOTE_NAME }",
		Short: "Print access token for a remote, login if necessary",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cfs := conffs.NewStore(dir, conffs.LocalSource, "")
			c, err := cfs.Open()
			if err != nil {
				return err
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}

			uriS := args[0]
			if v, ok := c.Remote[uriS]; ok {
				uriS = v.URL
			}
			uriS = strings.TrimRight(uriS, "/")
			uri, err := url.Parse(uriS)
			if err != nil {
				return err
			}
			if tok := cs.GetTokenMatching(*uri); tok != "" {
				cmd.Println(tok)
			}

			cm := utils.NewClientMap(cs)
			client, err := cm.GetClient(cmd, uriS, apiclient.WithRelyingPartyToken("invalid token"))
			if err != nil {
				return err
			}
			if _, err = client.GetRefs(nil, nil); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}
