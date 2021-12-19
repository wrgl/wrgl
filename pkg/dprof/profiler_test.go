// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dprof

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/objects"
)

func floatPtr(f float64) *float64 {
	return &f
}

func TestProfiler(t *testing.T) {
	columns := []string{"A", "B", "C"}
	rows := [][]string{
		{"", "abc", "1"},
		{"", "def", "2"},
		{"2", "qwe", "3-A"},
		{"30", "abc", "5-D"},
		{"4", "", "4-C"},
	}
	p := NewProfiler(columns)
	for _, row := range rows {
		p.Process(row)
	}
	assert.Equal(t, &objects.TableSummary{
		RowsCount: 5,
		Columns: []*objects.ColumnSummary{
			{
				Name:      "A",
				NullCount: 2,
				Min:       floatPtr(2),
				Max:       floatPtr(30),
				Mean:      floatPtr(7.2),
				IsNumber:  true,
			},
			{
				Name:      "B",
				NullCount: 1,
				AvgStrLen: 2,
				TopValues: objects.ValueCounts{
					{Value: "abc", Count: 2},
					{Value: "def", Count: 1},
					{Value: "qwe", Count: 1},
				},
			},
			{
				Name:      "C",
				AvgStrLen: 2,
			},
		},
	}, p.Summarize())
}

func TestPercentiles(t *testing.T) {
	columns := []string{"A"}
	rows := [][]string{
		{"8081"}, {"7887"}, {"1847"}, {"4059"}, {"2081"}, {"1318"}, {"4425"}, {"2540"}, {"456"}, {"3300"},
		{"694"}, {"8511"}, {"8162"}, {"5089"}, {"4728"}, {"3274"}, {"1211"}, {"1445"}, {"3237"}, {"9106"},
		{"495"}, {"5466"}, {"1528"}, {"6258"}, {"8047"}, {"9947"}, {"8287"}, {"2888"}, {"2790"}, {"3015"},
		{"5541"}, {"408"}, {"7387"}, {"6831"}, {"5429"}, {"5356"}, {"1737"}, {"631"}, {"1485"}, {"5026"},
		{"6413"}, {"3090"}, {"5194"}, {"563"}, {"2433"}, {"4147"}, {"4078"}, {"4324"}, {"6159"}, {"1353"},
		{"1957"}, {"3721"}, {"7189"}, {"2199"}, {"3000"}, {"8705"}, {"2888"}, {"4538"}, {"9703"}, {"9355"},
		{"2451"}, {"8510"}, {"2605"}, {"156"}, {"8266"}, {"9828"}, {"5561"}, {"7202"}, {"4783"}, {"5746"},
		{"1563"}, {"4376"}, {"9002"}, {"9718"}, {"5447"}, {"5094"}, {"1577"}, {"7463"}, {"7996"}, {"6420"},
		{"8623"}, {"953"}, {"1137"}, {"3133"}, {"9241"}, {"59"}, {"3033"}, {"8643"}, {"3891"}, {"2002"},
		{"8878"}, {"9336"}, {"2546"}, {"9107"}, {"7940"}, {"6503"}, {"552"}, {"9843"}, {"2205"}, {"1598"},
	}
	p := NewProfiler(columns)
	for _, row := range rows {
		p.Process(row)
	}
	assert.Equal(t, &objects.ColumnSummary{
		Name:     "A",
		IsNumber: true,
		Min:      floatPtr(59),
		Max:      floatPtr(9947),
		Mean:     floatPtr(4739.99),
		TopValues: objects.ValueCounts{
			{Value: "2888", Count: 2},
			{Value: "1137", Count: 1},
			{Value: "1211", Count: 1},
			{Value: "1318", Count: 1},
			{Value: "1353", Count: 1},
			{Value: "1445", Count: 1},
			{Value: "1485", Count: 1},
			{Value: "1528", Count: 1},
			{Value: "156", Count: 1},
			{Value: "1563", Count: 1},
			{Value: "1577", Count: 1},
			{Value: "1598", Count: 1},
			{Value: "1737", Count: 1},
			{Value: "1847", Count: 1},
			{Value: "1957", Count: 1},
			{Value: "2002", Count: 1},
			{Value: "2081", Count: 1},
			{Value: "2199", Count: 1},
			{Value: "2205", Count: 1},
			{Value: "2433", Count: 1},
		},
		AvgStrLen: 3,
		Percentiles: []float64{
			552, 1137, 1485, 1737, 2199, 2546, 3000, 3237, 4059, 4425, 5094, 5447, 6159, 6831, 7887, 8162, 8623, 9106, 9703,
		},
	}, p.Summarize().Columns[0])
}
