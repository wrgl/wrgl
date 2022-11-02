// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func CmdAuthError(cmd *cobra.Command, remoteURL string, err error) error {
	if strings.HasPrefix(remoteURL, api.APIRoot) {
		remoteURL = api.APIRoot
	}
	return fmt.Errorf("%w\nRun this command to authenticate:\n    wrgl credentials authenticate %s", err, remoteURL)
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
	if v := apiclient.UnwrapHTTPError(err); v != nil && (v.Code == http.StatusForbidden || v.Code == http.StatusUnauthorized) {
		if uri != nil {
			if err := discardCredentials(cmd, cs, uri); err != nil {
				return err
			}
		}
		return CmdAuthError(cmd, remoteURL, err)
	}
	return err
}

func ErrTableNotFound(db objects.Store, rs ref.Store, commit *objects.Commit) error {
	if remote, err := apiclient.FindRemoteFor(db, rs, commit.Sum); err != nil {
		return err
	} else if remote == "" {
		return fmt.Errorf("table %x not found", commit.Table)
	} else {
		return fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables %s %x", commit.Table, remote, commit.Table)
	}
}

func GetTable(db objects.Store, rs ref.Store, commit *objects.Commit) (*objects.Table, error) {
	tbl, err := objects.GetTable(db, commit.Table)
	if err != nil {
		if err == objects.ErrKeyNotFound {
			return nil, ErrTableNotFound(db, rs, commit)
		}
		return nil, fmt.Errorf("objects.GetTable err: %v", err)
	}
	return tbl, nil
}

var wrglhubRemoteRegex = regexp.MustCompile(`^https://hub\.wrgl\.co/api/users/([^/]+)/repos/([^/]+)`)

func IsWrglhubRemote(s string) (username, reponame string, ok bool) {
	if m := wrglhubRemoteRegex.FindStringSubmatch(s); m != nil {
		return m[1], m[2], true
	}
	return "", "", false
}

func RepoWebURI(username, reponame string) string {
	return fmt.Sprintf("https://hub.wrgl.co/@%s/r/%s/", username, reponame)
}
