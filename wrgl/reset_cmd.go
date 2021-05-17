package main

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func newResetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset BRANCH COMMIT",
		Short: "Reset branch head commit to the specified commit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			branch := args[0]
			rd := getRepoDir(cmd)
			wrglDir := utils.MustWRGLDir(cmd)
			conf, err := versioning.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			fs := rd.OpenFileStore()
			_, hash, _, err := versioning.InterpretCommitName(kvStore, args[1], false)
			if err != nil {
				return err
			}
			if len(hash) == 0 {
				return fmt.Errorf("commit \"%s\" not found", args[1])
			}
			return versioning.SaveRef(kvStore, fs, "heads/"+branch, hash, conf.User.Name, conf.User.Email, "reset", "to commit "+hex.EncodeToString(hash))
		},
	}
	return cmd
}
