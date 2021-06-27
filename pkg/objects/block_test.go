package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func randomBlock() [][]string {
	blk := make([][]string, 128)
	for i := 0; i < 128; i++ {
		blk[i] = make([]string, 10)
		for j := 0; j < 10; j++ {
			blk[i][j] = testutils.BrokenRandomAlphaNumericString(5)
		}
	}
	return blk
}

func TestBlockWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := NewBlockWriter(buf)
	blk := randomBlock()
	n, err := w.Write(blk)
	require.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))
	r := NewBlockReader(bytes.NewReader(b))
	blk2, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, blk, blk2)
}
