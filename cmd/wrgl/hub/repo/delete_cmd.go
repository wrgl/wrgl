// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package repo

import (
	"fmt"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete NAME...",
		Short: "Remove one or more repository.",
		Long:  "Remote one or more repository. This also wipes the data completely.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, tok, err := getWrglHubCreds(cmd)
			if err != nil {
				return err
			}
			user, err := api.GetMe(tok)
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, api.APIRoot, err)
			}
			quiet, err := cmd.Flags().GetBool("quiet")
			if err != nil {
				return err
			}
			n := len(args)
			var pb *progressbar.ProgressBar
			if n > 1 {
				pb = utils.PBar(int64(n), "Removing repositories", cmd.OutOrStdout(), cmd.ErrOrStderr())
				defer pb.Finish()
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
					pb.Add(1)
				} else {
					cmd.Printf("Deleted repository %q\n", repo)
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolP("quiet", "q", false, "don't ask for confirmation")
	return cmd
}
