package doctor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestCommitMap(t *testing.T) {
	m := commitMap{}
	sum1 := testutils.SecureRandomBytes(16)
	sum2 := testutils.SecureRandomBytes(16)
	m.update(sum1, sum2)

	com := &objects.Commit{}
	assert.False(t, m.parentsUpdated(com))
	assert.Empty(t, com.Parents)

	sum3 := testutils.SecureRandomBytes(16)
	com = &objects.Commit{
		Parents: [][]byte{sum3},
	}
	assert.False(t, m.parentsUpdated(com))
	assert.Equal(t, [][]byte{sum3}, com.Parents)

	com = &objects.Commit{
		Parents: [][]byte{sum2},
	}
	assert.False(t, m.parentsUpdated(com))
	assert.Equal(t, [][]byte{sum2}, com.Parents)

	com = &objects.Commit{
		Parents: [][]byte{sum1},
	}
	assert.True(t, m.parentsUpdated(com))
	assert.Equal(t, [][]byte{sum2}, com.Parents)
}
