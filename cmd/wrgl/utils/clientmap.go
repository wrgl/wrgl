// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"net/url"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/ref"
)

type ClientMap struct {
	CredsStore *credentials.Store
	clients    map[string]*apiclient.Client
	refs       map[string]map[string][]byte
	logger     logr.Logger
}

func NewClientMap(cs *credentials.Store, logger logr.Logger) *ClientMap {
	return &ClientMap{
		CredsStore: cs,
		logger:     logger.WithName("ClientMap").V(1),
		clients:    map[string]*apiclient.Client{},
		refs:       map[string]map[string][]byte{},
	}
}

func (m *ClientMap) GetClient(cmd *cobra.Command, remoteURI string, opts ...apiclient.ClientOption) (client *apiclient.Client, err error) {
	if v, ok := m.clients[remoteURI]; ok {
		m.logger.Info("found client", "remoteURI", remoteURI)
		return v, nil
	} else {
		m.logger.Info("client not found", "remoteURI", remoteURI)
	}
	u, err := url.Parse(remoteURI)
	if err != nil {
		return
	}
	opts = append(opts, apiclient.WithUMATicketHandler(func(asURI, ticket, oldRPT string, logger logr.Logger) (rpt string, err error) {
		return handleUMATicket(cmd, m.CredsStore, *u, asURI, ticket, oldRPT, logger)
	}))
	rpt := m.CredsStore.GetRPT(*u)
	if rpt != "" {
		m.logger.Info("rpt found", "remoteURI", remoteURI)
		opts = append(opts, apiclient.WithRelyingPartyToken(rpt))
	} else {
		m.logger.Info("rpt not found", "remoteURI", remoteURI)
	}
	client, err = apiclient.NewClient(remoteURI, m.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("error creating new client: %w", err)
	}
	m.logger.Info("memoizing client", "remoteURI", remoteURI)
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
