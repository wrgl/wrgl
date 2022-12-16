// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pckhoi/uma/pkg/httputil"
	"github.com/pckhoi/uma/pkg/rp"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func authenticateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "authenticate { REMOTE_URI | REMOTE_NAME } CLIENT_ID CLIENT_SECRET_FILE",
		Short: "Authenticate a remote with client id and secret.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "authenticate remote using client id and secret",
				Line:    "wrgl credentials authenticate origin my-client ./client_secret.txt",
			},
		}),
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			httpCli := utils.GetClient(cmd.Context())
			remote, clientID, clientSecretFile := args[0], args[1], args[2]
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
			uriS, uri, err := getRemoteURI(remote, c)
			if err != nil {
				return err
			}
			clientSecretB, err := os.ReadFile(clientSecretFile)
			if err != nil {
				return err
			}
			clientSecret := strings.TrimSpace(string(clientSecretB))

			req, err := http.NewRequest(http.MethodPost, uriS+"/commits/", nil)
			if err != nil {
				return err
			}
			resp, err := httpCli.Do(req)
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusUnauthorized {
				return httputil.NewErrUnanticipatedResponse(resp)
			}
			asURIStr, ticket := apiclient.ExtractTicketFrom401(resp)
			if asURIStr == "" || ticket == "" {
				return fmt.Errorf("unable to extract UMA ticket from %s", uriS)
			}
			asURIStr = strings.TrimRight(asURIStr, "/")
			asURI, err := url.Parse(asURIStr)
			if err != nil {
				return err
			}
			cmd.Println("extracted UMA ticket")

			saClient, err := rp.NewKeycloakClient(asURIStr, clientID, clientSecret, httpCli)
			if err != nil {
				return fmt.Errorf("error creating new keycloak client: %w", err)
			}
			creds, err := saClient.Authenticate()
			if err != nil {
				return fmt.Errorf("error authenticating keycloak client: %w", err)
			}
			cmd.Println("authenticated client")
			cs.SetAccessToken(*asURI, creds.AccessToken, creds.RefreshToken)

			rpt, err := saClient.RequestRPT(creds.AccessToken, rp.RPTRequest{
				Ticket: ticket,
			})
			if err != nil {
				return fmt.Errorf("error requesting rpt: %w", err)
			}
			cmd.Println("requested RPT")
			cs.SetRPT(*uri, rpt)

			defer cmd.Println("saved credentials")
			return cs.Flush()
		},
	}
	return cmd
}
