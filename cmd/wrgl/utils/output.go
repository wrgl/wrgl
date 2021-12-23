// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func PrintAuthCmd(cmd *cobra.Command, remoteURL string) {
	if strings.HasPrefix(remoteURL, "https://hub.wrgl.co/api") {
		remoteURL = "https://hub.wrgl.co/api"
	}
	cmd.Printf("Run this command to authenticate:\n    wrgl credentials authenticate %s\n", remoteURL)
}

func discardCredentials(cmd *cobra.Command, cs *credentials.Store, uri *url.URL) error {
	if uri == nil {
		return nil
	}
	cmd.Printf("Discarding credentials for %s\n", uri.String())
	cs.Delete(*uri)
	return cs.Flush()
}

func HandleHTTPError(cmd *cobra.Command, cs *credentials.Store, remoteURL string, uri *url.URL, err error) error {
	if v, ok := err.(*apiclient.HTTPError); ok && v.Code == http.StatusUnauthorized {
		if uri != nil {
			cmd.Println("Credentials are invalid")
			if err := discardCredentials(cmd, cs, uri); err != nil {
				return err
			}
		} else {
			cmd.Println("Unauthorized.")
		}
		PrintAuthCmd(cmd, remoteURL)
	}
	return err
}
