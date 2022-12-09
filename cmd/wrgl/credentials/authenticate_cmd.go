// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func authenticateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "authenticate { REMOTE_URI | REMOTE_NAME } TOKEN_FILE",
		Short: "Authenticate for one or more remotes with an access token file.",
		Long:  "Authenticate for one or more remotes with an access token file and save for future use. If REMOTE_NAME is given, then login and save credentials for that remote. If REMOTE_URI is given, then remote name is looked up from it.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "authenticate origin using token",
				Line:    "wrgl credentials authenticate origin ./token.txt",
			},
		}),
		Args: cobra.ExactArgs(2),
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
			_, uri, err := getRemoteURI(args[0], c)
			if err != nil {
				return err
			}
			token, err := os.ReadFile(args[1])
			if err != nil {
				return err
			}
			cs.Set(*uri, strings.TrimSpace(string(token)))
			return cs.Flush()
		},
	}
	return cmd
}
