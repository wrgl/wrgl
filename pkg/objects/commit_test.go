// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
)

func TestWriteCommit(t *testing.T) {
	c := objhelpers.RandomCommit()
	buf := bytes.NewBufferString("")
	n, err := c.WriteTo(buf)
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	n, c2, err := objects.ReadCommitFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	objhelpers.AssertCommitEqual(t, c, c2)
}
