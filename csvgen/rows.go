// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"math/rand"
	"sort"
	"time"
)

func randomValue() string {
	return brokenRandomAlphaNumericString(4 + rand.Intn(8))
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

func addRows(modifiedRows map[int]struct{}, numModRows int, rows [][]string) [][]string {
	offs := selectIndices(modifiedRows, numModRows, len(rows)-1+numModRows)
	n := len(rows[0])
	for _, off := range offs {
		k := 1 + off
		rows = append(rows[:k+1], rows[k:]...)
		rows[k] = make([]string, n)
		for j := 0; j < n; j++ {
			rows[k][j] = randomValue()
		}
	}
	return rows
}

func removeRows(modifiedRows map[int]struct{}, numModRows int, rows [][]string) [][]string {
	offs := selectIndices(cloneIntMap(modifiedRows), numModRows, len(rows)-1)
	for i := len(offs) - 1; i >= 0; i-- {
		off := offs[i] + 1
		rows = append(rows[:off], rows[off+1:]...)
	}
	return rows
}

func modifyRows(modifiedRows map[int]struct{}, numModRows int, rows [][]string) [][]string {
	offs := selectIndices(modifiedRows, numModRows, len(rows)-1)
	for _, off := range offs {
		off++
		l := oneFifth(len(rows[0]) - 1)
		inds := selectIndices(nil, l, len(rows[0])-1)
		for _, i := range inds {
			rows[off][i+1] = randomValue()
		}
	}
	return rows
}
