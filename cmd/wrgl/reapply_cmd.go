package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func reapplyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reapply {COMMIT BRANCH | TRANSACTION_ID}",
		Short: "Reapply a commit on a branch or reapply an entire transaction.",
		Long:  "Reapply a commit on a branch or reapply an entire transaction. This command does not alter history but rather add new commits on top of the latest commits.",
		Args:  cobra.RangeArgs(1, 2),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "reapply a commit",
				Line:    "wrgl reapply df141fa202642740b73df25e55c0082e main",
			},
			{
				Comment: "reapply a transaction",
				Line:    "wrgl reapply e392847b-52ac-448d-9607-718d10f5c43d",
			},
		}),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			if len(args) == 1 {
				id, err := uuid.Parse(args[0])
				if err != nil {
					return err
				}
				tx, err := rs.GetTransaction(id)
				if err != nil {
					return fmt.Errorf("transaction not found")
				}
				if tx.Status != ref.TSCommitted {
					return fmt.Errorf("transaction not committed")
				}
				cmd.Printf("Reapplying transaction %s\n", id)
				return transaction.Reapply(db, rs, id, func(branch string, sum []byte, message string) {
					if sum == nil {
						cmd.Printf(
							"branch %s has not changed since\n\n",
							branch,
						)
					} else {
						cmd.Printf(
							"[%s %s]\n    %s\n\n",
							branch,
							hex.EncodeToString(sum)[:7],
							strings.Replace(strings.TrimSpace(message), "\n", "\n    ", -1),
						)
					}
				})
			} else {
				return reapplyCommit(cmd, db, rs, args[0], args[1])
			}
		},
	}
	return cmd
}

func reapplyCommit(cmd *cobra.Command, db objects.Store, rs ref.Store, sumHex, branch string) error {
	sum, err := hex.DecodeString(sumHex)
	if err != nil {
		return err
	}
	oldSum, err := ref.GetHead(rs, branch)
	if err != nil {
		return err
	}
	if bytes.Equal(oldSum, sum) {
		cmd.Printf(
			"branch %s is already set to this commit\n",
			branch,
		)
		return nil
	}
	origCom, err := objects.GetCommit(db, sum)
	if err != nil {
		return err
	}
	com := &objects.Commit{
		Time:        time.Now(),
		AuthorName:  origCom.AuthorName,
		AuthorEmail: origCom.AuthorEmail,
		Table:       origCom.Table,
		Message:     fmt.Sprintf("reapply [com/%x]\n%s", sum, origCom.Message),
		Parents:     [][]byte{oldSum},
	}
	buf := bytes.NewBuffer(nil)
	_, err = com.WriteTo(buf)
	if err != nil {
		return err
	}
	newSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return err
	}
	if err = ref.SaveRef(rs, ref.HeadRef(branch), newSum, com.AuthorName, com.AuthorEmail, "reapply", fmt.Sprintf("commit %x", sum), nil); err != nil {
		return err
	}
	cmd.Printf(
		"[%s %s]\n    %s\n\n",
		branch,
		hex.EncodeToString(newSum)[:7],
		strings.Replace(strings.TrimSpace(com.Message), "\n", "\n    ", -1),
	)
	return nil
}
