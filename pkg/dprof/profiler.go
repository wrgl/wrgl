// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

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
)

type Profiler struct {
	columns     []*objects.ColumnSummary
	rowsCount   uint32
	strLens     []int
	sums        []float64
	numbers     []map[float64]uint32
	valueCounts []map[string]uint32
}

func NewProfiler(columnNames []string) *Profiler {
	n := len(columnNames)
	m := &Profiler{
		columns:     make([]*objects.ColumnSummary, n),
		strLens:     make([]int, n),
		valueCounts: make([]map[string]uint32, n),
		numbers:     make([]map[float64]uint32, n),
		sums:        make([]float64, n),
	}
	for i, name := range columnNames {
		m.columns[i] = &objects.ColumnSummary{
			Name:     name,
			IsNumber: true,
		}
		m.valueCounts[i] = map[string]uint32{}
		m.numbers[i] = map[float64]uint32{}
	}
	return m
}

func (m *Profiler) Process(row []string) {
	m.rowsCount += 1
	for i, col := range m.columns {
		v := row[i]
		m.strLens[i] += len(v)
		if v == "" {
			col.NullCount++
			continue
		}
		if col.IsNumber {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				col.IsNumber = false
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

func (m *Profiler) setStandardDeviation(i int, mean float64) {
	var sum float64
	for v, c := range m.numbers[i] {
		sum += (v - mean) * (v - mean) * float64(c)
	}
	f := math.Sqrt(sum / float64(m.rowsCount))
	m.columns[i].StdDeviation = &f
}

func (m *Profiler) Summarize() *objects.TableSummary {
	for i, col := range m.columns {
		col.AvgStrLen = uint16(uint32(m.strLens[i]) / m.rowsCount)
		if col.IsNumber {
			col.Mean = floatPtr(m.sums[i] / float64(m.rowsCount))
			var median, mode float64
			mode, median, col.Percentiles = m.calculatePercentiles(i)
			col.Median = &median
			col.Mode = &mode
			m.setStandardDeviation(i, *col.Mean)
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
	return &objects.TableSummary{
		RowsCount: m.rowsCount,
		Columns:   m.columns,
	}
}
