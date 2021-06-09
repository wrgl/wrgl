// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"math/rand"
	"time"
)

func oneFifth(n int) int {
	m := n / 5
	if m == 0 {
		m = 1
	}
	return m
}

func insertAt(sl []string, i int, s string) []string {
	sl = append(sl[:i+1], sl[i:]...)
	sl[i] = s
	return sl
}

func randomColName() string {
	return "col_" + brokenRandomLowerAlphaString(4)
}

func addColumns(modifiedCols map[string]struct{}, numColMods int, rows [][]string) [][]string {
	for i := 0; i < numColMods; i++ {
		name := randomColName()
		modifiedCols[name] = struct{}{}
		j := rand.Intn(len(rows[0]))
		rows[0] = insertAt(rows[0], j, name)
		for k := len(rows) - 1; k >= 1; k-- {
			rows[k] = insertAt(rows[k], j, randomValue())
		}
	}
	return rows
}

func selectColumns(modifiedCols map[string]struct{}, columns []string) int {
	rand.Seed(time.Now().UnixNano())
	for {
		j := rand.Intn(len(columns))
		name := columns[j]
		if _, ok := modifiedCols[name]; !ok {
			modifiedCols[name] = struct{}{}
			return j
		}
	}
}

func cloneStringMap(m map[string]struct{}) map[string]struct{} {
	res := map[string]struct{}{}
	for k := range m {
		res[k] = struct{}{}
	}
	return res
}

func remColumns(modifiedCols map[string]struct{}, numColMods int, rows [][]string) [][]string {
	for i := 0; i < numColMods; i++ {
		j := selectColumns(cloneStringMap(modifiedCols), rows[0])
		for k := len(rows) - 1; k >= 0; k-- {
			rows[k] = append(rows[k][:j], rows[k][j+1:]...)
		}
	}
	return rows
}

func renameColumns(modifiedCols map[string]struct{}, numColMods int, rows [][]string) [][]string {
	for i := 0; i < numColMods; i++ {
		j := selectColumns(modifiedCols, rows[0])
		name := randomColName()
		modifiedCols[name] = struct{}{}
		rows[0][j] = name
	}
	return rows
}

func moveColumns(modifiedCols map[string]struct{}, numColMods int, rows [][]string) [][]string {
	for i := 0; i < numColMods; i++ {
		j := selectColumns(modifiedCols, rows[0])
		var l int
		for {
			l = rand.Intn(len(rows[0]))
			if l != j {
				break
			}
		}
		for k := len(rows) - 1; k >= 0; k-- {
			v := rows[k][j]
			rows[k] = append(rows[k][:j], rows[k][j+1:]...)
			rows[k] = insertAt(rows[k], l, v)
		}
	}
	return rows
}

func genColumns(n int) []string {
	cols := make([]string, n)
	for i := 0; i < n; i++ {
		col := []byte("col_")
		if i < 25 {
			col = append(col, byte(i+97))
			cols[i] = string(col)
			continue
		}
		chars := []byte{}
		for k := i; k > 0; k = k / 25 {
			chars = append(chars, byte(k-(k/25)*25))
		}
		for j := len(chars) - 1; j >= 0; j-- {
			col = append(col, chars[j]+97)
		}
		cols[i] = string(col)
	}
	return cols
}
