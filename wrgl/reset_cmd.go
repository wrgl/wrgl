package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset BRANCH COMMIT",
		Short: "Reset branch head commit to the specified commit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			branch := args[0]
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			hash, _, _, err := versioning.InterpretCommitName(kvStore, args[1])
			if err != nil {
				return err
			}
			if len(hash) == 0 {
				return fmt.Errorf("commit \"%s\" not found", args[1])
			}
			return versioning.SaveHead(kvStore, branch, hash)
		},
	}
	return cmd
}
