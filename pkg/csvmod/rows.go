// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package csvmod

import (
	"math/rand"
	"sort"
	"time"
)

func randomValue() string {
	return BrokenRandomAlphaNumericString(4 + rand.Intn(8))
}

func selectIndex(modifiedIndices map[int]struct{}, n int) int {
	rand.Seed(time.Now().UnixNano())
	for {
		j := rand.Intn(n)
		if _, ok := modifiedIndices[j]; !ok {
			modifiedIndices[j] = struct{}{}
			return j
		}
	}
}

func selectIndices(modifiedIndices map[int]struct{}, numModRows, n int) []int {
	if modifiedIndices == nil {
		modifiedIndices = map[int]struct{}{}
	}
	offs := make([]int, numModRows)
	for i := 0; i < numModRows; i++ {
		j := selectIndex(modifiedIndices, n)
		offs[i] = j
	}
	sort.Ints(offs)
	return offs
}

func cloneIntMap(m map[int]struct{}) map[int]struct{} {
	res := map[int]struct{}{}
	for k := range m {
		res[k] = struct{}{}
	}
	return res
}

func (m *Modifier) AddRows(pct float64) *Modifier {
	numModRows := int(float64(m.nRows) * pct)
	offs := selectIndices(m.modifiedRows, numModRows, len(m.Rows)-1+numModRows)
	n := len(m.Rows[0])
	for _, off := range offs {
		row := make([]string, n)
		for j := 0; j < n; j++ {
			row[j] = randomValue()
		}
		k := 1 + off
		if k == cap(m.Rows) {
			m.Rows = append(m.Rows, row)
		} else {
			m.Rows = append(m.Rows[:k+1], m.Rows[k:]...)
			m.Rows[k] = row
		}
	}
	return m
}

func (m *Modifier) RemoveRows(pct float64) *Modifier {
	numModRows := int(float64(m.nRows) * pct)
	offs := selectIndices(cloneIntMap(m.modifiedRows), numModRows, len(m.Rows)-1)
	for i := len(offs) - 1; i >= 0; i-- {
		off := offs[i] + 1
		m.Rows = append(m.Rows[:off], m.Rows[off+1:]...)
	}
	return m
}

func (m *Modifier) ModifyRows(pct float64) *Modifier {
	numModRows := int(float64(m.nRows) * pct)
	numModCols := int(float64(m.nCols-1) * pct)
	offs := selectIndices(m.modifiedRows, numModRows, len(m.Rows)-1)
	for _, off := range offs {
		off++
		inds := selectIndices(nil, numModCols, len(m.Rows[0])-1)
		for _, i := range inds {
			m.Rows[off][i+1] = randomValue()
		}
	}
	return m
}
