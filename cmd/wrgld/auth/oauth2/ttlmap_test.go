package authoauth2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTTLMap(t *testing.T) {
	m := NewTTLMap(50 * time.Millisecond)
	m.StartCleanUpRoutine()
	defer m.Stop()

	m.Add("abc", 123, time.Millisecond*10)
	assert.Equal(t, 123, m.Pop("abc"))
	assert.Nil(t, m.Pop("abc"))

	m.Add("def", 456, time.Millisecond*10)
	m.Add("qwe", 234, time.Millisecond*220)
	time.Sleep(time.Millisecond * 200)
	assert.Nil(t, m.Pop("def"))
	assert.Equal(t, 234, m.Pop("qwe"))
}
