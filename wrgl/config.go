package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/imdario/mergo"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const localConfigName = ".wrglconfig.yaml"

type ConfigUser struct {
	Email string `yaml:"email,omitempty" json:"email,omitempty"`
	Name  string `yaml:"name,omitempty" json:"name,omitempty"`
}

type Config struct {
	User *ConfigUser `yaml:"user,omitempty" json:"user,omitempty"`
	path string      `yaml:"-" json:"-"`
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

func findLocalConfigFile() (string, error) {
	var childDir string
	var configRoot string
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir != childDir {
		if _, err := os.Stat(filepath.Join(dir, localConfigName)); os.IsNotExist(err) {
			childDir = dir
			dir = filepath.Dir(dir)
			continue
		}
		configRoot = dir
		break
	}
	if configRoot == "" {
		configRoot = dir
	}
	return filepath.Join(configRoot, localConfigName), nil
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

func openConfig(global bool, file string) (*Config, error) {
	var (
		fp  string
		err error
	)
	if file != "" {
		fp = file
	} else if global {
		fp, err = findGlobalConfigFile()
		if err != nil {
			return nil, err
		}
	} else {
		fp, err = findLocalConfigFile()
		if err != nil {
			return nil, err
		}
	}
	return readConfig(fp)
}

func aggregateConfig(cmd *cobra.Command) (*Config, error) {
	fp, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return nil, err
	}
	fileConfig, err := readConfig(fp)
	if err != nil {
		return nil, err
	}
	fp, err = findLocalConfigFile()
	if err != nil {
		return nil, err
	}
	localConfig, err := readConfig(fp)
	if err != nil {
		return nil, err
	}
	fp, err = findGlobalConfigFile()
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
	err = mergo.Merge(fileConfig, localConfig)
	if err != nil {
		return nil, err
	}
	fileConfig.path = ""
	out := cmd.ErrOrStderr()
	if fileConfig.User == nil || fileConfig.User.Email == "" {
		fmt.Fprintln(out, "User config not set. Set your user config with like this:")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, `  wrgl config --global user.email "john-doe@domain.com"`)
		fmt.Fprintln(out, `  wrgl config --global user.name "John Doe"`)
		os.Exit(1)
	}
	return fileConfig, nil
}
