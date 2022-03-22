package transaction

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list transactions sorted by begin time",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			rs := rd.OpenRefStore()
			out, cleanOut, err := utils.PagerOrOut(cmd)
			if err != nil {
				return err
			}
			defer cleanOut()
			var off int
			limit := 20
			zone, offset := time.Now().Zone()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			txs, err := rs.ListTransactions(off, limit)
			if err != nil {
				return err
			}
			for _, tx := range txs {
				fmt.Fprintf(out, "transaction %s\n", tx.ID)
				fmt.Fprintf(out, "Status: %s\n", string(tx.Status))
				fmt.Fprintf(out, "Begin: %s\n", tx.Begin.In(time.FixedZone(zone, offset)))
				if !tx.End.IsZero() {
					fmt.Fprintf(out, "End: %s\n", tx.End.In(time.FixedZone(zone, offset)))
				}
				fmt.Fprintln(out)
				refs, err := ref.ListTransactionRefs(rs, tx.ID)
				if err != nil {
					return err
				}
				for branch, sum := range refs {
					com, err := objects.GetCommit(db, sum)
					if err != nil {
						return err
					}
					fmt.Fprintf(out, "    [%s %s] %s\n", branch, hex.EncodeToString(sum)[:7], ref.FirstLine(com.Message))
				}
				fmt.Fprintln(out)
			}
			return nil
		},
	}
	cmd.Flags().BoolP("no-pager", "P", false, "don't use PAGER")
	return cmd
}
