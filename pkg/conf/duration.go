// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package conf

import "time"

type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(data []byte) error {
	o, err := time.ParseDuration(string(data))
	if err != nil {
		return err
	}
	*d = Duration(o)
	return nil
}
