// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"net/url"

	"github.com/spf13/cobra"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/ref"
)

type ClientMap struct {
	CredsStore *credentials.Store
	clients    map[string]*apiclient.Client
	refs       map[string]map[string][]byte
}

func NewClientMap(cs *credentials.Store) *ClientMap {
	return &ClientMap{
		CredsStore: cs,
		clients:    map[string]*apiclient.Client{},
		refs:       map[string]map[string][]byte{},
	}
}

func (m *ClientMap) GetClient(cmd *cobra.Command, remoteURI string, opts ...apiclient.ClientOption) (client *apiclient.Client, err error) {
	if v, ok := m.clients[remoteURI]; ok {
		return v, nil
	}
	u, err := url.Parse(remoteURI)
	if err != nil {
		return
	}
	opts = append(opts, apiclient.WithUMATicketHandler(func(asURI, ticket, oldRPT string) (rpt string, err error) {
		return handleUMATicket(cmd, m.CredsStore, *u, asURI, ticket, oldRPT)
	}))
	if tok, err := GetCredentials(cmd, m.CredsStore, remoteURI); err != nil {
		return nil, err
	} else if tok != "" {
		opts = append(opts, apiclient.WithRelyingPartyToken(tok))
	}
	client, err = GetAPIClient(cmd, remoteURI, opts...)
	if err != nil {
		return nil, err
	}
	m.clients[remoteURI] = client
	return
}

func (m *ClientMap) GetRefs(cmd *cobra.Command, remoteURI string, opts ...apiclient.ClientOption) (refs map[string][]byte, err error) {
	if v, ok := m.refs[remoteURI]; ok {
		return v, nil
	}
	client, err := m.GetClient(cmd, remoteURI, opts...)
	if err != nil {
		return
	}
	refs, err = client.GetRefs(nil, []string{ref.TransactionRefPrefix})
	if err != nil {
		return nil, HandleHTTPError(cmd, m.CredsStore, remoteURI, err)
	}
	return
}
