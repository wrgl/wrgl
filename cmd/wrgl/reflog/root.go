// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package reflog

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/ref"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reflog REFERENCE",
		Short: "Show the logs of REFERENCE",
		Long:  "Show the logs of REFERENCE. REFERENCE can be complete reference like \"refs/heads/main\" or shorten to just \"main\".",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "show logs for main branch",
				Line:    "wrgl reflog heads/main",
			},
			{
				Comment: "show logs for remote tracking branch",
				Line:    "wrgl reflog remotes/origin/main",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
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
				if errors.Is(err, io.EOF) {
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
