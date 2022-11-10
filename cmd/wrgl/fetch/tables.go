// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package fetch

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newTablesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tables { REMOTE SUM... | --missing }",
		Short: "Download missing tables from another repository",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "fetch 2 missing tables from origin",
				Line:    "wrgl fetch tables origin 639c229dd42c53e03d716eaa0829916b a29a4d9a6c445eeb4b32c929d8c1e669",
			},
			{
				Comment: "fetch all missing tables",
				Line:    "wrgl fetch tables --missing",
			},
		}),
		Args: cobra.ArbitraryArgs,
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
			missing, err := cmd.Flags().GetBool("missing")
			if err != nil {
				return err
			}
			cm, err := utils.NewClientMap()
			if err != nil {
				return err
			}

			if missing {
				rs := rd.OpenRefStore()
				heads, err := ref.ListHeads(rs)
				if err != nil {
					return err
				}
				if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
					for _, sum := range heads {
						q, err := ref.NewCommitsQueue(db, [][]byte{sum})
						if err != nil {
							return err
						}
						coms := []*objects.Commit{}
						for {
							_, com, err := q.PopInsertParents()
							if com != nil {
								coms = append(coms, com)
							}
							if err != nil {
								if err == io.EOF {
									break
								}
								return err
							}
						}
						if shallowErr := apiclient.NewShallowCommitError(db, rs, coms); shallowErr != nil {
							if err = fetchTableSums(cmd, db, cm, c, shallowErr.TableSums, barContainer); err != nil {
								return err
							}
						}
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			} else {
				remote := args[0]
				sums := make([][]byte, len(args)-1)
				for i, s := range args[1:] {
					sums[i], err = hex.DecodeString(s)
					if err != nil {
						return fmt.Errorf("error decoding hex string %q: %v", s, err)
					}
				}
				return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
					return fetchTableSums(cmd, db, cm, c, map[string][][]byte{remote: sums}, barContainer)
				})
			}
		},
	}
	cmd.Flags().Bool("missing", false, "Fetch all missing tables that could be reached from a branch")
	return cmd
}

func deduplicateSums(sums [][]byte) [][]byte {
	m := map[string]struct{}{}
	for _, s := range sums {
		m[string(s)] = struct{}{}
	}
	sl := make([][]byte, 0, len(m))
	for s := range m {
		sl = append(sl, []byte(s))
	}
	return sl
}

func fetchTableSums(cmd *cobra.Command, db objects.Store, cm *utils.ClientMap, c *conf.Config, tblSums map[string][][]byte, pbarContainer pbar.Container) (err error) {
	for remote, sums := range tblSums {
		sums = deduplicateSums(sums)
		rem, ok := c.Remote[remote]
		if !ok {
			return fmt.Errorf("remote %q not found", remote)
		}
		client, uri, err := cm.GetClient(cmd, rem)
		if err != nil {
			return err
		}
		pr, err := client.GetObjects(sums)
		if err != nil {
			return utils.HandleHTTPError(cmd, cm.CredsStore, rem.URL, uri, err)
		}
		defer pr.Close()
		or := apiutils.NewObjectReceiver(db, nil)
		bar := pbarContainer.NewBar(-1, "Fetching objects", 0)
		defer bar.Done()
		_, err = or.Receive(pr, bar)
		if err != nil {
			return err
		}
		bar.Done()
		for _, b := range sums {
			cmd.Printf("Table %x persisted\n", b)
		}
	}
	return nil
}
