// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conffs

import (
	"fmt"
	"os"
	"path/filepath"
)

func systemConfigPath() string {
	if s := os.Getenv("WRGL_SYSTEM_CONFIG_DIR"); s != "" {
		return filepath.Join(s, "config.yaml")
	}
	return "/usr/local/etc/wrgl/config.yaml"
}

func localPath(rootDir string) string {
	return filepath.Join(rootDir, "config.yaml")
}

func (s *Store) path() (string, error) {
	switch s.source {
	case SystemSource:
		return systemConfigPath(), nil
	case GlobalSource:
		return globalConfigPath()
	case LocalSource:
		return localPath(s.rootDir), nil
	case FileSource:
		return s.fp, nil
	default:
		return "", fmt.Errorf("unrecognized source: %v", s.source)
	}
}
