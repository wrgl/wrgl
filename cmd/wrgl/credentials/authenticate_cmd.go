// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func authenticateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "authenticate { REMOTE_URI | REMOTE_NAME }",
		Short: "Authenticate for one or more remotes with email/password.",
		Long:  "Authenticate for one or more remotes with email/password and save credentials for future use. If REMOTE_NAME is given, then login and save credentials for that remote. If REMOTE_URI is given, login at REMOTE_URI/authenticate/ and save credentials for all remotes that have REMOTE_URI as prefix.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "authenticate for origin",
				Line:    "wrgl credentials authenticate origin",
			},
			{
				Comment: "authenticate for all repositories on wrgl hub",
				Line:    "wrgl credentials authenticate " + api.APIRoot,
			},
			{
				Comment: "authenticate from token",
				Line:    fmt.Sprintf("wrgl credentials authenticate %s --token-location ./token.txt", api.APIRoot),
			},
		}),
		Args: cobra.ExactArgs(1),
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
			tokLoc, err := cmd.Flags().GetString("token-location")
			if err != nil {
				return err
			}
			uriS := args[0]
			if v, ok := c.Remote[uriS]; ok {
				uriS = v.URL
			}
			uriS = strings.TrimRight(uriS, "/")
			u, err := url.Parse(uriS)
			if err != nil {
				return err
			}
			if tokLoc != "" {
				token, err := os.ReadFile(tokLoc)
				if err != nil {
					return err
				}
				cs.Set(*u, strings.TrimSpace(string(token)))
				return cs.Flush()
			}
			return getCredentials(cmd, cs, uriS, u)
		},
	}
	cmd.Flags().String("token-location", "", "read and save auth token from this location")
	return cmd
}

func getCredentials(cmd *cobra.Command, cs *credentials.Store, uriS string, u *url.URL) (err error) {
	s, err := discoverAuthServer(uriS)
	if err != nil {
		return
	}
	clientID := "wrgl"
	accessToken, err := s.Authenticate(cmd, clientID)
	if err != nil {
		return
	}
	rpt, err := s.RequestRPT(cmd, accessToken, clientID)
	if err != nil {
		return
	}
	cs.Set(*u, rpt)
	if err = cs.Flush(); err != nil {
		return fmt.Errorf("flush err: %v", err)
	}
	cmd.Printf("Saved credentials to %s\n", cs.Path())
	return nil
}
