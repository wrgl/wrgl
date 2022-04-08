// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"net/url"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/fetch"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/credentials"
)

type clientMap struct {
	CredsStore *credentials.Store
	clients    map[string]*apiclient.Client
	uris       map[string]*url.URL
	refs       map[string]map[string][]byte
}

func newClientMap() (*clientMap, error) {
	cs, err := credentials.NewStore()
	if err != nil {
		return nil, err
	}
	return &clientMap{
		CredsStore: cs,
		clients:    map[string]*apiclient.Client{},
		uris:       map[string]*url.URL{},
		refs:       map[string]map[string][]byte{},
	}, nil
}

func (m *clientMap) GetClient(cmd *cobra.Command, cr *conf.Remote) (client *apiclient.Client, uri *url.URL, err error) {
	if v, ok := m.clients[cr.URL]; ok {
		return v, m.uris[cr.URL], nil
	}
	uri, tok, err := fetch.GetCredentials(cmd, m.CredsStore, cr.URL)
	if err != nil {
		return nil, nil, err
	}
	m.uris[cr.URL] = uri
	client, err = apiclient.NewClient(cr.URL, apiclient.WithAuthorization(tok))
	if err != nil {
		return nil, nil, err
	}
	m.clients[cr.URL] = client
	return
}

func (m *clientMap) GetRefs(cmd *cobra.Command, cr *conf.Remote) (refs map[string][]byte, err error) {
	if v, ok := m.refs[cr.URL]; ok {
		return v, nil
	}
	client, uri, err := m.GetClient(cmd, cr)
	if err != nil {
		return
	}
	refs, err = client.GetRefs("")
	if err != nil {
		return nil, utils.HandleHTTPError(cmd, m.CredsStore, cr.URL, uri, err)
	}
	return
}
