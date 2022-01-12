// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestUint16Stat(t *testing.T) {
	f := uint16StatFactory("Min length", "minStrLen", func(col *objects.ColumnProfile) uint16 { return col.MinStrLen })
	s := f(
		nil, nil, &objects.ColumnProfile{MinStrLen: 10}, &objects.ColumnProfile{MinStrLen: 20},
	)
	assert.Equal(t, &Uint16Stat{
		Name:      "Min length",
		ShortName: "minStrLen",
		Old:       20,
		New:       10,
	}, s)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "{\"name\":\"Min length\",\"shortName\":\"minStrLen\",\"old\":20,\"new\":10}", string(b))
	assert.Nil(t, f(nil, nil, nil, nil))
	assert.Nil(t, f(nil, nil, &objects.ColumnProfile{}, &objects.ColumnProfile{}))
}

func TestUint32Stat(t *testing.T) {
	f := uint32StatFactory("NA count", "naCount", true, func(col *objects.ColumnProfile) uint32 { return col.NACount })
	s := f(
		nil, nil, &objects.ColumnProfile{NACount: 10}, &objects.ColumnProfile{NACount: 20},
	)
	assert.Equal(t, &Uint32Stat{
		Name:      "NA count",
		ShortName: "naCount",
		Old:       20,
		New:       10,
	}, s)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "{\"name\":\"NA count\",\"shortName\":\"naCount\",\"old\":20,\"new\":10}", string(b))
	assert.Nil(t, f(nil, nil, nil, nil))
	assert.Equal(t, f(nil, nil, &objects.ColumnProfile{}, &objects.ColumnProfile{}), &Uint32Stat{
		Name:      "NA count",
		ShortName: "naCount",
	})
}

func floatPtr(v float64) *float64 {
	return &v
}

func TestFloat64Stat(t *testing.T) {
	f := float64StatFactory("Mean", "mean", func(col *objects.ColumnProfile) *float64 { return col.Mean })
	s := f(
		nil, nil, &objects.ColumnProfile{Mean: floatPtr(10)}, &objects.ColumnProfile{Mean: floatPtr(20)},
	)
	assert.Equal(t, &Float64Stat{
		Name:      "Mean",
		ShortName: "mean",
		Old:       floatPtr(20),
		New:       floatPtr(10),
	}, s)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "{\"name\":\"Mean\",\"shortName\":\"mean\",\"old\":20,\"new\":10}", string(b))
	assert.Nil(t, f(nil, nil, nil, nil))
	assert.Nil(t, f(nil, nil, &objects.ColumnProfile{}, &objects.ColumnProfile{}))
}
