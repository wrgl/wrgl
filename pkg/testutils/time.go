// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package testutils

import "time"

// createTimeGen create a time generator that returns a timestamp that increase by 1 second
// each time it is called. This ensures that all commits have different timestamp.
func CreateTimeGen() func() time.Time {
	t := time.Now()
	return func() time.Time {
		t = t.Add(time.Second)
		return t
	}
}
