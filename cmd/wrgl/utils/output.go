// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
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
	if v, ok := err.(*apiclient.HTTPError); ok && v.Code == http.StatusForbidden {
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

type RemoteFinder struct {
	travs map[string]*ref.Traveller
}

func NewRemoteFinder(db objects.Store, rs ref.Store) (*RemoteFinder, error) {
	m, err := ref.ListAllRefs(rs)
	if err != nil {
		return nil, err
	}
	travellers := map[string]*ref.Traveller{}
	for name := range m {
		travellers[name], err = ref.NewTraveller(db, rs, name)
		if err != nil {
			return nil, err
		}
	}
	return &RemoteFinder{
		travs: travellers,
	}, nil
}

func (f *RemoteFinder) FindRemoteFor(sum []byte) (string, error) {
	if sum == nil {
		return "", fmt.Errorf("empty commit sum")
	}
	for len(f.travs) > 0 {
		for name, t := range f.travs {
			com, err := t.Next()
			if err != nil {
				return "", err
			}
			if com == nil {
				t.Close()
				delete(f.travs, name)
			} else if bytes.Equal(com.Sum, sum) {
				if remote := t.Reflog.FetchRemote(); remote != "" {
					return remote, nil
				}
				t.Close()
				delete(f.travs, name)
			}
		}
	}
	return "", nil
}

func (f *RemoteFinder) Close() error {
	for _, t := range f.travs {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

func FindRemoteFor(db objects.Store, rs ref.Store, sum []byte) (string, error) {
	f, err := NewRemoteFinder(db, rs)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return f.FindRemoteFor(sum)
}

func ErrTableNotFound(db objects.Store, rs ref.Store, commit *objects.Commit) error {
	if remote, err := FindRemoteFor(db, rs, commit.Sum); err != nil {
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
		return nil, err
	}
	return tbl, nil
}
