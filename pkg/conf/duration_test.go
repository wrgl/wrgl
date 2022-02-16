package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	d := new(Duration)
	require.NoError(t, d.UnmarshalText([]byte("72h3m0.5s")))
	assert.Equal(t, Duration(259380500000000), *d)
	b, err := d.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, []byte("72h3m0.5s"), b)
}
