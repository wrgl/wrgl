package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/testutils"
)

func TestBlockBuffer(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
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
	buf, err := newBlockBuffer(db, db, tbl1, tbl2)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			row, err := buf.getRow(0, uint32(i), byte(j))
			require.NoError(t, err)
			assert.Equal(t, row, rows1[i*3+j+1])
			row, err = buf.getRow(1, uint32(i), byte(j))
			require.NoError(t, err)
			assert.Equal(t, row, rows2[i*3+j+1])
		}
	}
}
