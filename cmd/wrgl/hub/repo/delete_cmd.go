// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package repo

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/pbar"
)

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete NAME...",
		Short: "Remove one or more repository.",
		Long:  "Remote one or more repository. This also wipes the data completely.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, uri, tok, err := getWrglHubCreds(cmd)
			if err != nil {
				return err
			}
			user, err := api.GetMe(tok)
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, api.APIRoot, uri, err)
			}
			quiet, err := cmd.Flags().GetBool("quiet")
			if err != nil {
				return err
			}
			n := len(args)
			return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
				var pb pbar.Bar
				if n > 1 {
					if !quiet {
						return fmt.Errorf("removing more than one repository. Use flag --quiet to suppress prompt")
					}
					pb = barContainer.NewBar(int64(n), "Removing repositories", 0)
					defer pb.Done()
				}
				for _, repo := range args {
					if !quiet {
						val, err := utils.Prompt(cmd, fmt.Sprintf("Type repo name (%s) for confirmation", repo))
						if err != nil {
							return err
						}
						if val != repo {
							return fmt.Errorf("input mismatch %q != %q", val, repo)
						}
					}
					if err = api.DeleteRepo(tok, user.Username, repo); err != nil {
						return err
					}
					if n > 1 {
						pb.Incr()
					} else {
						cmd.Printf("Deleted repository %q\n", repo)
					}
				}
				return nil
			})
		},
	}
	cmd.Flags().BoolP("quiet", "q", false, "don't ask for confirmation")
	return cmd
}
