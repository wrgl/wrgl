package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset BRANCH COMMIT",
		Short: "Reset branch's head commit to the specified commit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			branch := args[0]
			rd := getRepoDir(cmd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			hash, _, _, err := versioning.InterpretCommitName(kvStore, args[1])
			if err != nil {
				return err
			}
			if hash == "" {
				return fmt.Errorf("commit \"%s\" not found", args[1])
			}
			b := &versioning.Branch{CommitHash: hash}
			return b.Save(kvStore, branch)
		},
	}
	return cmd
}
