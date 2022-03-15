// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diffprof

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestPercentilesStat(t *testing.T) {
	f := percentilesStatFactory("Percentiles", "percentiles", func(col *objects.ColumnProfile) []float64 { return col.Percentiles })
	s := f(nil, nil,
		&objects.ColumnProfile{Percentiles: []float64{1, 2, 3, 4}},
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8}},
	)
	assert.Equal(t, &PercentilesStat{
		Name:      "Percentiles",
		ShortName: "percentiles",
		Values: []*PercentileDiff{
			{Old: 5, New: 1},
			{Old: 6, New: 2},
			{Old: 7, New: 3},
			{Old: 8, New: 4},
		},
	}, s)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t,
		"{\"name\":\"Percentiles\",\"shortName\":\"percentiles\",\"values\":[{\"old\":5,\"new\":1},{\"old\":6,\"new\":2},{\"old\":7,\"new\":3},{\"old\":8,\"new\":4}]}",
		string(b),
	)

	assert.Equal(t, &PercentilesStat{
		Name:      "Percentiles",
		ShortName: "percentiles",
		Values: []*PercentileDiff{
			{New: 1},
			{New: 2},
			{New: 3},
			{New: 4},
		},
	}, f(nil, nil,
		&objects.ColumnProfile{Percentiles: []float64{1, 2, 3, 4}},
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8, 9, 10}},
	))

	assert.Equal(t, &PercentilesStat{
		Name:        "Percentiles",
		ShortName:   "percentiles",
		NewAddition: true,
		Values: []*PercentileDiff{
			{New: 1},
			{New: 2},
			{New: 3},
			{New: 4},
		},
	}, f(nil, nil,
		&objects.ColumnProfile{Percentiles: []float64{1, 2, 3, 4}},
		&objects.ColumnProfile{},
	))

	assert.Equal(t, &PercentilesStat{
		Name:        "Percentiles",
		ShortName:   "percentiles",
		NewAddition: true,
		Values: []*PercentileDiff{
			{New: 1},
			{New: 2},
			{New: 3},
			{New: 4},
		},
	}, f(nil, nil,
		&objects.ColumnProfile{Percentiles: []float64{1, 2, 3, 4}},
		nil,
	))

	assert.Equal(t, &PercentilesStat{
		Name:      "Percentiles",
		ShortName: "percentiles",
		Removed:   true,
		Values: []*PercentileDiff{
			{Old: 5},
			{Old: 6},
			{Old: 7},
			{Old: 8},
		},
	}, f(nil, nil,
		&objects.ColumnProfile{},
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8}},
	))

	assert.Equal(t, &PercentilesStat{
		Name:      "Percentiles",
		ShortName: "percentiles",
		Removed:   true,
		Values: []*PercentileDiff{
			{Old: 5},
			{Old: 6},
			{Old: 7},
			{Old: 8},
		},
	}, f(nil, nil,
		nil,
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8}},
	))

	assert.Equal(t, &PercentilesStat{
		Name:      "Percentiles",
		ShortName: "percentiles",
		Values: []*PercentileDiff{
			{Old: 5, New: 5},
			{Old: 6, New: 6},
			{Old: 7, New: 7},
			{Old: 8, New: 8},
		},
	}, f(nil, nil,
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8}},
		&objects.ColumnProfile{Percentiles: []float64{5, 6, 7, 8}},
	))
}
