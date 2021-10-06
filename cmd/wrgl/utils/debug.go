package utils

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

// SetupDebug open debug file (specified with --debug-file) for writing
func SetupDebug(cmd *cobra.Command) (w io.Writer, cleanup func(), err error) {
	name, err := cmd.Flags().GetString("debug-file")
	if err != nil {
		return nil, nil, err
	}
	var f *os.File
	if name != "" {
		f, err = os.Create(name)
		if err != nil {
			return nil, nil, err
		}
	}
	return f, func() {
		if f != nil {
			f.Close()
		}
	}, nil
}
