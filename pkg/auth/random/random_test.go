// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package random

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomAlphaNumericString(t *testing.T) {
	s := RandomAlphaNumericString(10)
	assert.Len(t, s, 10)
}
