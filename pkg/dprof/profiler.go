// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dprof

import (
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

type numberCount struct {
	v float64
	c uint32
	s uint32
}

func (m *Profiler) calculatePercentiles(i int) []float64 {
	sl := make([]*numberCount, 0, len(m.numbers[i]))
	for v, c := range m.numbers[i] {
		sl = append(sl, &numberCount{v, c, 0})
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].v < sl[j].v
	})
	var sum uint32
	for _, nc := range sl {
		nc.s = sum
		sum += nc.c
	}
	// if there are less distinct values than percentile slot then don't calculate percentiles
	if sum < 100/PercentileIncrement {
		return nil
	}
	percentiles := make([]float64, 0, 100/PercentileIncrement-1)
	for k := PercentileIncrement; k < 100; k += PercentileIncrement {
		c := float64(k) / 100 * float64(sum)
		i := sort.Search(len(sl), func(i int) bool {
			return float64(sl[i].s) >= c
		})
		if float64(sl[i].s) == c {
			percentiles = append(percentiles, sl[i].v)
		} else {
			// interpolate percentile
			percentiles = append(percentiles,
				sl[i-1].v+
					(float64(sl[i].s)-c)/
						float64(sl[i].s-sl[i-1].s)*
						(sl[i].v-sl[i-1].v),
			)
		}
	}
	return percentiles
}

func (m *Profiler) Summarize() *objects.TableSummary {
	for i, col := range m.columns {
		col.AvgStrLen = uint16(uint32(m.strLens[i]) / m.rowsCount)
		if col.IsNumber {
			f := m.sums[i] / float64(m.rowsCount)
			col.Mean = &f
			col.Percentiles = m.calculatePercentiles(i)
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
