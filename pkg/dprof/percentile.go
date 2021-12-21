// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package dprof

import "sort"

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

func (sl numberCounts) mode() float64 {
	sort.Slice(sl, func(i, j int) bool {
		if sl[i].count == sl[j].count {
			return sl[i].number < sl[j].number
		}
		return sl[i].count < sl[j].count
	})
	return sl[len(sl)-1].number
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

func (m *Profiler) calculatePercentiles(i int) (mode, median float64, percentiles []float64) {
	sl := numberCountsFromMap(m.numbers[i])
	median = sl.percentile(50)
	// if there are less distinct values than percentile slot then don't calculate percentiles
	if sl[len(sl)-1].cumCount >= 100/PercentileIncrement {
		percentiles = make([]float64, 0, 100/PercentileIncrement-1)
		for k := PercentileIncrement; k < 100; k += PercentileIncrement {
			percentiles = append(percentiles, sl.percentile(k))
		}
	}
	mode = sl.mode()
	return
}
