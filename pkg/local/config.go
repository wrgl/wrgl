// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package local

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/imdario/mergo"
	"github.com/wrgl/core/pkg/conf"
	"gopkg.in/yaml.v3"
)

func systemConfigPath() string {
	if s := os.Getenv("WRGL_SYSTEM_CONFIG_DIR"); s != "" {
		return filepath.Join(s, "config.yaml")
	}
	return "/usr/local/etc/wrgl/config.yaml"
}

func SaveConfig(c *conf.Config) error {
	if c.Path == "" {
		return fmt.Errorf("empty config path")
	}
	err := os.MkdirAll(filepath.Dir(c.Path), 0755)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(c.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

func findGlobalWindowConfigFile() (string, error) {
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

func findGlobalConfigFile() (string, error) {
	configDir := os.Getenv("XDG_CONFIG_HOME")
	if configDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		configDir = filepath.Join(homeDir, ".config")
	}
	return filepath.Join(configDir, "wrgl", "config.yaml"), nil
}

func readConfig(fp string) (*conf.Config, error) {
	c := &conf.Config{}
	if fp == "" {
		return c, nil
	}
	f, err := os.Open(fp)
	if err == nil {
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		err = yaml.Unmarshal(b, c)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	c.Path = fp
	return c, nil
}

func OpenConfig(system, global bool, rootDir, file string) (c *conf.Config, err error) {
	var fp string
	if file != "" {
		fp = file
	} else if system {
		fp = systemConfigPath()
	} else if global {
		fp, err = findGlobalConfigFile()
		if err != nil {
			return nil, err
		}
	} else {
		fp = filepath.Join(rootDir, "config.yaml")
	}
	return readConfig(fp)
}

type ptrTransformer struct {
}

func (t *ptrTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ.Kind() == reflect.Ptr && typ.Elem().Kind() != reflect.Struct {
		return func(dst, src reflect.Value) error {
			if dst.CanSet() && !src.IsNil() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}

func AggregateConfig(rootDir string) (*conf.Config, error) {
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
	sysConfig, err := readConfig(systemConfigPath())
	if err != nil {
		return nil, err
	}
	err = mergo.Merge(globalConfig, localConfig, mergo.WithOverride, mergo.WithTransformers(&ptrTransformer{}))
	if err != nil {
		return nil, err
	}
	err = mergo.Merge(sysConfig, globalConfig, mergo.WithOverride, mergo.WithTransformers(&ptrTransformer{}))
	if err != nil {
		return nil, err
	}
	// disable save
	sysConfig.Path = ""
	return sysConfig, nil
}
