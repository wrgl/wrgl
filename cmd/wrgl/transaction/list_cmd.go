package transaction

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
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
			for {
				txs, err := rs.ListTransactions(off, limit)
				if err != nil {
					return err
				}
				for _, tx := range txs {
					fmt.Fprintf(out, "transaction %s\n", tx.ID)
					switch tx.Status {
					case ref.TSCommitted:
						fmt.Fprint(out, "Status: committed\n")
					case ref.TSInProgress:
						fmt.Fprint(out, "Status: in-progress\n")
					}
					fmt.Fprintf(out, "Begin: %s\n", tx.Begin.In(time.FixedZone(zone, offset)))
					if tx.End.IsZero() {
						fmt.Fprintf(out, "End: %s\n", tx.End.In(time.FixedZone(zone, offset)))
					}
					fmt.Fprintln(out)
				}
			}
		},
	}
	cmd.Flags().BoolP("no-pager", "P", false, "don't use PAGER")
	return cmd
}
