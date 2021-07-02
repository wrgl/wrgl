// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

type ConfigUser struct {
	Email string `yaml:"email,omitempty" json:"email,omitempty"`
	Name  string `yaml:"name,omitempty" json:"name,omitempty"`
}

type ConfigReceive struct {
	DenyNonFastForwards *bool `yaml:"denyNonFastForwards,omitempty" json:"denyNonFastForwards,omitempty"`
	DenyDeletes         *bool `yaml:"denyDeletes,omitempty" json:"denyDeletes,omitempty"`
}

type ConfigBranch struct {
	Remote string `yaml:"remote,omitempty" json:"remote,omitempty"`
	Merge  string `yaml:"merge,omitempty" json:"merge,omitempty"`
}

type Config struct {
	User    *ConfigUser              `yaml:"user,omitempty" json:"user,omitempty"`
	Remote  map[string]*ConfigRemote `yaml:"remote,omitempty" json:"remote,omitempty"`
	Receive *ConfigReceive           `yaml:"receive,omitempty" json:"receive,omitempty"`
	Branch  map[string]*ConfigBranch `yaml:"branch,omitempty" json:"branch,omitempty"`
	Path    string                   `yaml:"-" json:"-"`
}
