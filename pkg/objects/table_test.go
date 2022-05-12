// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/misc"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestTableReader(t *testing.T) {
	buf := misc.NewBuffer(nil)
	table := &Table{
		Columns:   []string{"a", "b", "c", "d"},
		PK:        []uint32{0, 1},
		RowsCount: 760,
		Blocks: [][]byte{
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
		},
		BlockIndices: [][]byte{
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
		},
	}
	n, err := table.WriteTo(buf)
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))

	// test ReadTable
	n, table2, err := ReadTableFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	assert.Equal(t, table, table2)
	assert.Equal(t, 760, int(table2.RowsCount))
	assert.Equal(t, []string{"a", "b", "c", "d"}, table2.Columns)
	assert.Equal(t, []uint32{0, 1}, table2.PK)
}

func TestTableReaderParseError(t *testing.T) {
	buf := misc.NewBuffer([]byte("columns "))
	buf.Write(NewStrListEncoder(true).Encode([]string{"a", "b", "c"}))
	buf.Write([]byte("\nbad input"))
	_, _, err := ReadTableFrom(bytes.NewReader(buf.Bytes()))
	assert.Equal(t, `error reading label "pk": parse error at pos=25: expected string "pk ", received "bad"`, err.Error())
}
