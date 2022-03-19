package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSemVer(t *testing.T) {
	for i, c := range []struct {
		Text string
		SV   *SemVer
		Err  string
	}{
		{
			Err: "invalid semver",
		},
		{
			Text: "a.b.c",
			Err:  "strconv.Atoi: parsing \"a\": invalid syntax",
		},
		{
			Text: "0..0",
			Err:  "strconv.Atoi: parsing \"\": invalid syntax",
		},
		{
			Text: "0.0.0",
			SV:   &SemVer{},
		},
		{
			Text: "1.2.3",
			SV:   &SemVer{1, 2, 3},
		},
		{
			Text: "13.12.11",
			SV:   &SemVer{13, 12, 11},
		},
	} {
		v, err := ParseSemVer(c.Text)
		if err != nil {
			assert.Equal(t, c.Err, err.Error(), "case %d", i)
		} else {
			assert.Equal(t, c.SV, v, "case %d", i)
			s, err := v.MarshalText()
			require.NoError(t, err)
			assert.Equal(t, c.Text, string(s))
		}
	}
}

func TestCompareSemVer(t *testing.T) {
	semvers := []*SemVer{
		{0, 0, 0},
		{1, 0, 0},
		{1, 2, 0},
		{1, 2, 3},
	}
	for i, v := range semvers[1:] {
		assert.Equal(t, 1, v.CompareTo(semvers[i]))
		assert.Equal(t, -1, semvers[i].CompareTo(v))
		assert.Equal(t, 0, v.CompareTo(v))
	}
}
