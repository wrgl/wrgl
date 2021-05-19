package reflog

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func existCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exist REF",
		Short: "checks whether a ref has a reflog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := args[0]
			wrglDir := utils.MustWRGLDir(cmd)
			rd := versioning.NewRepoDir(wrglDir, false, false)
			db, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer db.Close()
			fs := rd.OpenFileStore()
			name, _, _, err := versioning.InterpretCommitName(db, ref, true)
			if err != nil {
				return fmt.Errorf("no such ref: %q", ref)
			}
			fmt.Printf("name: %s\n", name)
			if ok := fs.Exist([]byte("logs/" + name)); !ok {
				return fmt.Errorf("reflog for %q does not exist", ref)
			}
			cmd.Printf("reflog for %q does exist\n", ref)
			return nil
		},
	}
	return cmd
}
