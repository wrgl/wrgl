package reflog

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reflog REFERENCE",
		Short: "show the logs of the REFERENCE",
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
				return err
			}
			if !strings.HasPrefix(name, "refs/heads/") && !strings.HasPrefix(name, "refs/remotes") {
				return fmt.Errorf("unknown ref %q", name)
			}
			r, err := fs.Reader([]byte("logs/" + name))
			if err != nil {
				return err
			}
			defer r.Close()
			reader, err := objects.NewReflogReader(r)
			if err != nil {
				return err
			}
			out, cleanOut, err := utils.PagerOrOut(cmd)
			if err != nil {
				return err
			}
			defer cleanOut()
			name = strings.TrimPrefix(name, "refs/heads/")
			for i := 0; ; i++ {
				rec, err := reader.Read()
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
