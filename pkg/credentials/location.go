package credentials

import (
	"os"
	"path/filepath"
)

func credsLocation() string {
	if s := os.Getenv("XDG_CONFIG_HOME"); s != "" {
		return filepath.Join(s, "wrgl", "credentials.yaml")
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "wrgl", "credentials.yaml")
}
