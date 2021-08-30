// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package reflog

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reflog REFERENCE",
		Short: "show the logs of the REFERENCE",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			rd := local.NewRepoDir(wrglDir, false, false)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			name, _, _, err := ref.InterpretCommitName(db, rs, args[0], true)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(name, "heads/") && !strings.HasPrefix(name, "remotes") {
				return fmt.Errorf("unknown ref %q", name)
			}
			r, err := rs.LogReader(name)
			if err != nil {
				return err
			}
			defer r.Close()
			out, cleanOut, err := utils.PagerOrOut(cmd)
			if err != nil {
				return err
			}
			defer cleanOut()
			name = strings.TrimPrefix(name, "heads/")
			for i := 0; ; i++ {
				rec, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "%s %s@{%d}: %s: %s\n", hex.EncodeToString(rec.NewOID)[:7], name, i, rec.Action, rec.Message)
			}
			return nil
		},
	}
	cmd.Flags().BoolP("no-pager", "P", false, "don't use PAGER")
	cmd.AddCommand(existCmd())
	return cmd
}
