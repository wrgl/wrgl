// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conf

type Store interface {
	Open() (*Config, error)
	Save(*Config) error
}
