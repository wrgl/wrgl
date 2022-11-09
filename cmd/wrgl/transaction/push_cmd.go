// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/fetch"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

func pushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push REMOTE TRANSACTION_ID",
		Short: "Push transaction to a remote.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, txid := args[0], args[1]
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rem, ok := c.Remote[remote]
			if !ok {
				return fmt.Errorf("remote %q not found", remote)
			}
			id, err := uuid.Parse(txid)
			if err != nil {
				return fmt.Errorf("invalid transaction id: %v", err)
			}
			rs := rd.OpenRefStore()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			tx, err := rs.GetTransaction(id)
			if err != nil {
				return fmt.Errorf("transaction not found")
			}
			txRefs, err := ref.ListTransactionRefs(rs, id)
			if err != nil {
				return err
			}
			if len(txRefs) == 0 {
				return fmt.Errorf("transaction is empty")
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			cmd.Printf("To %s\n", rem.URL)
			cm := utils.NewClientMap(cs)
			client, err := cm.GetClient(cmd, rem.URL)
			if err != nil {
				return err
			}
			_, err = client.CreateTransaction(&payload.CreateTransactionRequest{
				ID:     tx.ID.String(),
				Begin:  tx.Begin,
				End:    tx.End,
				Status: string(tx.Status),
			})
			if err != nil {
				return err
			}
			cmd.Printf("transaction %s created\n", tx.ID)
			remoteRefs, err := client.GetRefs(nil, []string{ref.TransactionRefPrefix})
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, rem.URL, err)
			}
			noP, err := cmd.Flags().GetBool("no-progress")
			if err != nil {
				return err
			}
			updates := map[string]*payload.Update{}
			for branch, sum := range txRefs {
				updates[ref.TransactionRef(txid, branch)] = &payload.Update{
					Sum: payload.BytesToHex(sum),
				}
			}
			ses, err := apiclient.NewReceivePackSession(db, rs, client, updates, remoteRefs, c.MaxPackFileSize())
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, rem.URL, err)
			}
			var pb pbar.Bar
			if !noP {
				pb = pbar.NewProgressBar(cmd.OutOrStdout(), -1, "Pushing objects", 0)
				defer pb.Done()
			}
			updates, err = ses.Start(pb)
			if err != nil {
				return err
			}
			if pb != nil {
				pb.Done()
			}
			for k, u := range updates {
				if u.ErrMsg != "" {
					fetch.DisplayRefUpdate(cmd, '!', "[remote rejected]", u.ErrMsg, k, k)
				} else {
					fetch.DisplayRefUpdate(cmd, '*', "[new reference]", "", k, k)
				}
			}
			return nil
		},
	}
	cmd.Flags().Bool("no-progress", false, "don't display progress bar")
	return cmd
}
