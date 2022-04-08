// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package branch

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config BRANCH",
		Short: "Print branch config or set branch config",
		Args:  cobra.ExactArgs(1),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "print branch config as JSON",
				Line:    "wrgl branch config my-branch",
			},
			{
				Comment: "make branch track a local file",
				Line:    "wrgl branch config my-branch --set-file my_data.csv --set-delimiter '|' --set-primary-key id",
			},
			{
				Comment: "set upstream for branch",
				Line:    "wrgl branch config my-branch --set-upstream-remote origin --set-upstream-dest my-branch",
			},
		}),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			s := conffs.NewStore(rd.FullPath, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			setFile, err := cmd.Flags().GetString("set-file")
			if err != nil {
				return err
			}
			setPrimaryKey, err := cmd.Flags().GetStringSlice("set-primary-key")
			if err != nil {
				return err
			}
			setDelimiter, err := utils.GetRuneFromFlag(cmd, "set-delimiter")
			if err != nil {
				return err
			}
			setUpstreamRemote, err := cmd.Flags().GetString("set-upstream-remote")
			if err != nil {
				return err
			}
			setUpstreamDest, err := cmd.Flags().GetString("set-upstream-dest")
			if err != nil {
				return err
			}

			if c.Branch == nil {
				c.Branch = map[string]*conf.Branch{}
			}
			branch, ok := c.Branch[args[0]]

			if setFile == "" && len(setPrimaryKey) == 0 && setDelimiter == 0 && setUpstreamRemote == "" && setUpstreamDest == "" {
				if !ok {
					return fmt.Errorf("branch %q not found", args[0])
				}
				b, err := json.MarshalIndent(branch, "", "    ")
				if err != nil {
					return err
				}
				cmd.Println(string(b))
				return nil
			}

			if !ok {
				branch = &conf.Branch{}
			}
			if setFile != "" {
				branch.File = setFile
			}
			if len(setPrimaryKey) > 0 {
				branch.PrimaryKey = setPrimaryKey
			}
			if setDelimiter != 0 {
				branch.Delimiter = setDelimiter
			}
			if setUpstreamRemote != "" {
				branch.Remote = setUpstreamRemote
			}
			if setUpstreamDest != "" {
				if _, err = conf.ParseRefspec(setUpstreamDest); err != nil {
					return fmt.Errorf("invalid upstream destination: %v", err)
				}
				branch.Merge = setUpstreamDest
			}
			c.Branch[args[0]] = branch
			return s.Save(c)
		},
	}
	cmd.Flags().String("set-file", "", "set branch.file config to a CSV file. If branch.file is set, then you don't need to specify CSV_FILE_PATH in subsequent commits to BRANCH.")
	cmd.Flags().StringSlice("set-primary-key", nil, "set branch.primaryKey. If branch.primaryKey is set, then you don't need to specify PRIMARY_KEY in subsequent commits to BRANCH.")
	cmd.Flags().String("set-delimiter", "", "set branch.delimiter. branch.delimiter tells Wrgl what delimiter to use when parsing branch.file")
	cmd.Flags().String("set-upstream-remote", "", "set branch.remote. When both branch.remote and branch.merge are set, you can run `wrgl pull BRANCH` without specifying remote and refspec")
	cmd.Flags().String("set-upstream-dest", "", "set branch.merge. When both branch.remote and branch.merge are set, you can run `wrgl pull BRANCH` without specifying remote and refspec")
	return cmd
}
