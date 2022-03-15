// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestBlockBuffer(t *testing.T) {
	db := objmock.NewStore()
	rows1 := testutils.BuildRawCSV(4, 9)
	tbl1, _ := createTableFromBlock(t, db, rows1[0], []uint32{0}, [][][]string{
		rows1[1:4],
		rows1[4:7],
		rows1[7:],
	})
	rows2 := testutils.BuildRawCSV(4, 9)
	tbl2, _ := createTableFromBlock(t, db, rows2[0], []uint32{0}, [][][]string{
		rows2[1:4],
		rows2[4:7],
		rows2[7:],
	})
	buf, err := NewBlockBuffer([]objects.Store{db, db}, []*objects.Table{tbl1, tbl2})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			row, err := buf.GetRow(0, uint32(i), byte(j))
			require.NoError(t, err)
			assert.Equal(t, row, rows1[i*3+j+1])
			row, err = buf.GetRow(1, uint32(i), byte(j))
			require.NoError(t, err)
			assert.Equal(t, row, rows2[i*3+j+1])
		}
	}
}
