// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package dprof

import (
	"math"
	"sort"
	"strconv"

	"github.com/wrgl/wrgl/pkg/objects"
)

const (
	MaxTopValues        = 20
	PercentileIncrement = 5
	profilerVersion     = 1
)

type Profiler struct {
	columns     []*objects.ColumnProfile
	rowsCount   uint32
	strLens     []int
	isNumber    []bool
	sums        []float64
	numbers     []map[float64]uint32
	valueCounts []map[string]uint32
}

func NewProfiler(columnNames []string) *Profiler {
	n := len(columnNames)
	m := &Profiler{
		columns:     make([]*objects.ColumnProfile, n),
		strLens:     make([]int, n),
		valueCounts: make([]map[string]uint32, n),
		isNumber:    make([]bool, n),
		numbers:     make([]map[float64]uint32, n),
		sums:        make([]float64, n),
	}
	for i, name := range columnNames {
		m.columns[i] = &objects.ColumnProfile{
			Name: name,
		}
		m.valueCounts[i] = map[string]uint32{}
		m.numbers[i] = map[float64]uint32{}
		m.isNumber[i] = true
	}
	return m
}

func (m *Profiler) Process(row []string) {
	m.rowsCount += 1
	for i, col := range m.columns {
		v := row[i]
		n := len(v)
		m.strLens[i] += n
		if uint16(n) > col.MaxStrLen {
			col.MaxStrLen = uint16(n)
		}
		if v == "" {
			col.NACount++
			continue
		}
		if col.MinStrLen == 0 || uint16(n) < col.MinStrLen {
			col.MinStrLen = uint16(n)
		}
		if m.isNumber[i] {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				m.isNumber[i] = false
				col.Min = nil
				col.Max = nil
			} else {
				m.sums[i] += n
				m.numbers[i][n] += 1
				if col.Max == nil {
					var m float64 = n
					col.Max = &m
				}
				if col.Min == nil {
					var m float64 = n
					col.Min = &m
				}
				if n > *col.Max {
					*col.Max = n
				} else if n < *col.Min {
					*col.Min = n
				}
			}
		}
		m.valueCounts[i][v] += 1
	}
}

func floatPtr(f float64) *float64 {
	return &f
}

func roundTwoDecimalPlaces(f float64) float64 {
	return math.Round(f*100) / 100
}

func (m *Profiler) setStandardDeviation(i int, mean float64) {
	var sum float64
	for v, c := range m.numbers[i] {
		sum += (v - mean) * (v - mean) * float64(c)
	}
	f := roundTwoDecimalPlaces(math.Sqrt(sum / float64(m.rowsCount)))
	m.columns[i].StdDeviation = &f
}

func (m *Profiler) Summarize() *objects.TableProfile {
	for i, col := range m.columns {
		col.AvgStrLen = uint16(math.Round(float64(m.strLens[i]) / float64(m.rowsCount-col.NACount)))
		if m.isNumber[i] && len(m.numbers[i]) > 0 {
			col.Mean = floatPtr(roundTwoDecimalPlaces(m.sums[i] / float64(m.rowsCount-col.NACount)))
			m.setStandardDeviation(i, *col.Mean)
			var median float64
			median, col.Percentiles = m.calculatePercentiles(i)
			col.Median = &median
		}

		allUnique := true
		for s, n := range m.valueCounts[i] {
			if n > 1 {
				allUnique = false
			}
			col.TopValues = append(col.TopValues, objects.ValueCount{
				Value: s,
				Count: n,
			})
		}
		if allUnique {
			col.TopValues = nil
		} else {
			sort.Sort(col.TopValues)
			if col.TopValues.Len() > MaxTopValues {
				col.TopValues = col.TopValues[:MaxTopValues]
			}
		}
	}
	return &objects.TableProfile{
		Version:   profilerVersion,
		RowsCount: m.rowsCount,
		Columns:   m.columns,
	}
}
