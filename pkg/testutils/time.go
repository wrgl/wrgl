// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package testutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// createTimeGen create a time generator that returns a timestamp that increase by 1 second
// each time it is called. This ensures that all commits have different timestamp.
func CreateTimeGen() func() time.Time {
	t := time.Now()
	return func() time.Time {
		t = t.Add(time.Second)
		return t
	}
}

func AssertTimeEqual(t *testing.T, expected, actual time.Time, msgAndArgs ...interface{}) {
	t.Helper()
	require.Equal(t, expected.Unix(), actual.Unix(), msgAndArgs...)
	require.Equal(t, expected.Format("-0700"), actual.Format("-0700"), msgAndArgs...)
}
