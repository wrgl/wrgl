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
	number   float64
	count    uint32
	cumCount uint32
}

type numberCounts []*numberCount

func (sl numberCounts) total() uint32 {
	return sl[len(sl)-1].cumCount
}

func (sl numberCounts) percentRank(i int) int {
	return int(100 / float64(sl.total()) * (float64(i) + 1 - 0.5))
}

func (sl numberCounts) percentile(p int) float64 {
	if p < sl.percentRank(0) {
		return sl[0].number
	}
	n := int(sl.total())
	if p > sl.percentRank(n-1) {
		return sl[len(sl)-1].number
	}
	i := sort.Search(len(sl), func(i int) bool {
		return sl.percentRank(int(sl[i].cumCount-1)) >= p
	})
	if i == 0 || p >= sl.percentRank(int(sl[i-1].cumCount)) {
		return sl[i].number
	}
	p1 := sl.percentRank(int(sl[i-1].cumCount - 1))
	return sl[i].number + float64(n*(p-p1))/100*(sl[i].number-sl[i-1].number)
}

func numberCountsFromMap(m map[float64]uint32) numberCounts {
	sl := make(numberCounts, 0, len(m))
	for v, c := range m {
		sl = append(sl, &numberCount{v, c, 0})
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].number < sl[j].number
	})
	var sum uint32
	for _, nc := range sl {
		sum += nc.count
		nc.cumCount = sum
	}
	return sl
}

func (m *Profiler) calculatePercentiles(i int) (median float64, percentiles []float64) {
	sl := numberCountsFromMap(m.numbers[i])
	median = sl.percentile(50)
	// if there are less distinct values than percentile slot then don't calculate percentiles
	if sl[len(sl)-1].cumCount < 100/PercentileIncrement {
		return
	}
	percentiles = make([]float64, 0, 100/PercentileIncrement-1)
	for k := PercentileIncrement; k < 100; k += PercentileIncrement {
		percentiles = append(percentiles, sl.percentile(k))
	}
	return
}

func floatPtr(f float64) *float64 {
	return &f
}

func (m *Profiler) Summarize() *objects.TableSummary {
	for i, col := range m.columns {
		col.AvgStrLen = uint16(uint32(m.strLens[i]) / m.rowsCount)
		if col.IsNumber {
			col.Mean = floatPtr(m.sums[i] / float64(m.rowsCount))
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
	return &objects.TableSummary{
		RowsCount: m.rowsCount,
		Columns:   m.columns,
	}
}
