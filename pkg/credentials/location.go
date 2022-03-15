// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

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
