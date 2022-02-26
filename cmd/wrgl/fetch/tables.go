package fetch

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
)

func newTablesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tables REMOTE SUM...",
		Short: "Download missing tables from another repository",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "fetch 2 missing tables from origin",
				Line:    "wrgl fetch tables origin 639c229dd42c53e03d716eaa0829916b a29a4d9a6c445eeb4b32c929d8c1e669",
			},
		}),
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := utils.EnsureUserSet(cmd, c); err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()

			remote := args[0]
			rem, ok := c.Remote[remote]
			if !ok {
				return fmt.Errorf("remote %q not found", remote)
			}
			sums := make([][]byte, len(args)-1)
			for i, s := range args[1:] {
				sums[i], err = hex.DecodeString(s)
				if err != nil {
					return fmt.Errorf("error decoding hex string %q: %v", s, err)
				}
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			uri, tok, err := GetCredentials(cmd, cs, rem.URL)
			if err != nil {
				return err
			}
			client, err := apiclient.NewClient(rem.URL, apiclient.WithAuthorization(tok))
			if err != nil {
				return err
			}
			pr, err := client.GetObjects(sums)
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, rem.URL, uri, err)
			}
			defer pr.Close()
			or := apiutils.NewObjectReceiver(db, nil, nil)
			// pbar := utils.PBar(0, "fetching objects", cmd.OutOrStdout(), cmd.ErrOrStderr())
			// defer pbar.Finish()
			_, err = or.Receive(pr, nil)
			if err != nil {
				return err
			}
			// pbar.Finish()
			for _, b := range sums {
				cmd.Printf("Table %x persisted\n", b)
			}
			return nil
		},
	}
	return cmd
}
