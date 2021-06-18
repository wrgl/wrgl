// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package csvmod

import (
	"math/rand"
	"time"
)

func insertAt(sl []string, i int, s string) []string {
	sl = append(sl[:i+1], sl[i:]...)
	sl[i] = s
	return sl
}

func randomColName() string {
	return "col_" + BrokenRandomLowerAlphaString(4)
}

type Modifier struct {
	modifiedCols map[string]struct{}
	modifiedRows map[int]struct{}
	Rows         [][]string
	nCols        int
	nRows        int
}

func NewModifier(rows [][]string) *Modifier {
	m := &Modifier{
		modifiedCols: map[string]struct{}{},
		modifiedRows: map[int]struct{}{},
		Rows:         rows,
		nCols:        len(rows[0]),
		nRows:        len(rows),
	}
	return m
}

func (m *Modifier) PreserveColumns(columns []string) *Modifier {
	for _, s := range columns {
		m.modifiedCols[s] = struct{}{}
	}
	return m
}

func (m *Modifier) AddColumns(pct float64) *Modifier {
	numColMods := int(float64(m.nCols) * pct)
	for i := 0; i < numColMods; i++ {
		name := randomColName()
		m.modifiedCols[name] = struct{}{}
		j := rand.Intn(len(m.Rows[0]))
		m.Rows[0] = insertAt(m.Rows[0], j, name)
		for k := len(m.Rows) - 1; k >= 1; k-- {
			m.Rows[k] = insertAt(m.Rows[k], j, randomValue())
		}
	}
	return m
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

func (m *Modifier) RemColumns(pct float64) *Modifier {
	numColMods := int(float64(m.nCols) * pct)
	for i := 0; i < numColMods; i++ {
		j := selectColumns(cloneStringMap(m.modifiedCols), m.Rows[0])
		for k := len(m.Rows) - 1; k >= 0; k-- {
			m.Rows[k] = append(m.Rows[k][:j], m.Rows[k][j+1:]...)
		}
	}
	return m
}

func (m *Modifier) RenameColumns(pct float64) *Modifier {
	numColMods := int(float64(m.nCols) * pct)
	for i := 0; i < numColMods; i++ {
		j := selectColumns(m.modifiedCols, m.Rows[0])
		name := randomColName()
		m.modifiedCols[name] = struct{}{}
		m.Rows[0][j] = name
	}
	return m
}

func (m *Modifier) MoveColumns(pct float64) *Modifier {
	numColMods := int(float64(m.nCols) * pct)
	for i := 0; i < numColMods; i++ {
		j := selectColumns(m.modifiedCols, m.Rows[0])
		var l int
		for {
			l = rand.Intn(len(m.Rows[0]))
			if l != j {
				break
			}
		}
		for k := len(m.Rows) - 1; k >= 0; k-- {
			v := m.Rows[k][j]
			m.Rows[k] = append(m.Rows[k][:j], m.Rows[k][j+1:]...)
			m.Rows[k] = insertAt(m.Rows[k], l, v)
		}
	}
	return m
}
