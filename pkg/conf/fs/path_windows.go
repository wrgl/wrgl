// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conffs

import (
	"os"
	"path/filepath"
)

func globalConfigPath() (string, error) {
	configDir := os.Getenv("LOCALAPPDATA")
	if configDir == "" {
		configDir = os.Getenv("APPDATA")
	}
	if configDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		configDir = filepath.Join(homeDir, "AppData", "Local")
	}
	return filepath.Join(configDir, "wrgl", "config.yaml"), nil
}
