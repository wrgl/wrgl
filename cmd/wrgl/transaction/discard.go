package transaction

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func discardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "discard TRANSACTION_ID",
		Short: "Discard a transaction.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			id, err := uuid.Parse(args[0])
			if err != nil {
				return fmt.Errorf("error parsing transaction id: %v", err)
			}
			if err = transaction.Discard(db, rs, id); err != nil {
				return fmt.Errorf("error discarding transaction: %v", err)
			}
			return nil
		},
	}
	return cmd
}
