package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertMigration(t *testing.T) {
	for i, c := range []struct {
		Slice     []migration
		Migration migration
		Expected  []migration
	}{
		{
			Migration: migration{
				SemVer: &SemVer{},
			},
			Expected: []migration{
				{
					SemVer: &SemVer{},
				},
			},
		},
		{
			Slice: []migration{
				{
					SemVer: &SemVer{0, 0, 1},
				},
			},
			Migration: migration{
				SemVer: &SemVer{0, 1, 0},
			},
			Expected: []migration{
				{
					SemVer: &SemVer{0, 0, 1},
				},
				{
					SemVer: &SemVer{0, 1, 0},
				},
			},
		},
		{
			Slice: []migration{
				{
					SemVer: &SemVer{0, 1, 0},
				},
			},
			Migration: migration{
				SemVer: &SemVer{0, 0, 9},
			},
			Expected: []migration{
				{
					SemVer: &SemVer{0, 0, 9},
				},
				{
					SemVer: &SemVer{0, 1, 0},
				},
			},
		},
		{
			Slice: []migration{
				{
					SemVer: &SemVer{0, 1, 0},
				},
				{
					SemVer: &SemVer{0, 2, 0},
				},
			},
			Migration: migration{
				SemVer: &SemVer{0, 1, 2},
			},
			Expected: []migration{
				{
					SemVer: &SemVer{0, 1, 0},
				},
				{
					SemVer: &SemVer{0, 1, 2},
				},
				{
					SemVer: &SemVer{0, 2, 0},
				},
			},
		},
	} {
		assert.Equal(t, c.Expected, insertMigration(c.Slice, c.Migration), "case %d", i)
	}
}
