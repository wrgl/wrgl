package errors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrap(t *testing.T) {
	err1 := fmt.Errorf("my new error")
	err2 := Wrap("err 2", err1)
	assert.Equal(t, "err 2: my new error", err2.Error())
	assert.Equal(t, err1, Unwrap(err2))
}

func TestContains(t *testing.T) {
	err1 := fmt.Errorf("my new error")
	err2 := Wrap("err 2", err1)
	err3 := fmt.Errorf("another error")
	assert.True(t, Contains(err1, err1))
	assert.True(t, Contains(err2, err1))
	assert.True(t, Contains(err1, err1.Error()))
	assert.True(t, Contains(err2, err1.Error()))
	assert.False(t, Contains(err3, err1))
	assert.False(t, Contains(err3, err1.Error()))
	assert.False(t, Contains(err1, 123))
	assert.True(t, Contains(nil, nil))
	assert.False(t, Contains(nil, ""))
	assert.False(t, Contains(err1, nil))
	assert.False(t, Contains(nil, err1))
}
