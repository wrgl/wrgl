// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package versioning

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/imdario/mergo"
	"gopkg.in/yaml.v3"
)

type ConfigUser struct {
	Email string `yaml:"email,omitempty" json:"email,omitempty"`
	Name  string `yaml:"name,omitempty" json:"name,omitempty"`
}

type ConfigReceive struct {
	DenyNonFastForwards bool `yaml:"denyNonFastForwards,omitempty" json:"denyNonFastForwards,omitempty"`
	DenyDeletes         bool `yaml:"denyDeletes,omitempty" json:"denyDeletes,omitempty"`
}

type Config struct {
	User    *ConfigUser              `yaml:"user,omitempty" json:"user,omitempty"`
	Remote  map[string]*ConfigRemote `yaml:"remote,omitempty" json:"remote,omitempty"`
	Receive *ConfigReceive           `yaml:"receive,omitempty" json:"receive,omitempty"`
	path    string                   `yaml:"-" json:"-"`
}

func (c *Config) Save() error {
	if c.path == "" {
		return fmt.Errorf("empty config path")
	}
	f, err := os.OpenFile(c.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}

func findGlobalConfigFile() (string, error) {
	var configDir string
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	if runtime.GOOS == "windows" {
		configDir = os.Getenv("LOCALAPPDATA")
		if configDir == "" {
			configDir = os.Getenv("APPDATA")
		}
		if configDir == "" {
			configDir = filepath.Join(homeDir, "AppData", "Local")
		}
	} else {
		configDir = os.Getenv("XDG_CONFIG_HOME")
		if configDir == "" {
			configDir = filepath.Join(homeDir, ".config")
		}
	}
	return filepath.Join(configDir, "wrgl", "config.yaml"), nil
}

func readConfig(fp string) (*Config, error) {
	if fp == "" {
		return &Config{}, nil
	}
	f, err := os.Open(fp)
	c := &Config{}
	if os.IsNotExist(err) {
	} else if err != nil {
		return nil, err
	} else {
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		err = yaml.Unmarshal(b, c)
		if err != nil {
			return nil, err
		}
	}
	c.path = fp
	return c, nil
}

func OpenConfig(global bool, rootDir string) (*Config, error) {
	var (
		fp  string
		err error
	)
	if global {
		fp, err = findGlobalConfigFile()
		if err != nil {
			return nil, err
		}
	} else {
		fp = filepath.Join(rootDir, "config.yaml")
	}
	return readConfig(fp)
}

func AggregateConfig(rootDir string) (*Config, error) {
	localConfig, err := readConfig(filepath.Join(rootDir, "config.yaml"))
	if err != nil {
		return nil, err
	}
	fp, err := findGlobalConfigFile()
	if err != nil {
		return nil, err
	}
	globalConfig, err := readConfig(fp)
	if err != nil {
		return nil, err
	}
	err = mergo.Merge(localConfig, globalConfig)
	if err != nil {
		return nil, err
	}
	localConfig.path = ""
	return localConfig, nil
}
