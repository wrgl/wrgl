// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package repo

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/api"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete NAME",
		Short: "Remove a repository.",
		Long:  "Remote a repository. This also wipes the data completely.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, uri, tok, err := getWrglHubCreds(cmd)
			if err != nil {
				return err
			}
			user, err := api.GetMe(tok)
			if err != nil {
				return utils.HandleHTTPError(cmd, cs, api.APIRoot, uri, err)
			}
			val, err := utils.Prompt(cmd, fmt.Sprintf("Type repo name (%s) for confirmation", args[0]))
			if err != nil {
				return err
			}
			if val != args[0] {
				return fmt.Errorf("input mismatch %q != %q", val, args[0])
			}
			if err = api.DeleteRepo(tok, user.Username, args[0]); err != nil {
				return err
			}
			cmd.Printf("Deleted repository %q\n", args[0])
			return nil
		},
	}
	return cmd
}
