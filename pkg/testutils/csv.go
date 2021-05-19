// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package testutils

import (
	"bytes"
	"encoding/csv"
	"math/rand"
	"strconv"
	"strings"
)

func BuildRawCSV(numCol, numRow int) [][]string {
	columns := []string{"id"}
	for i := 0; i < numCol-1; i++ {
		columns = append(columns, BrokenRandomAlphaNumericString(5))
	}
	rawCSV := [][]string{columns}
	for i := 0; i < numRow; i++ {
		row := []string{strconv.Itoa(i + 1)}
		for j := 0; j < numCol-1; j++ {
			row = append(row, BrokenRandomAlphaNumericString(rand.Intn(20)+1))
		}
		rawCSV = append(rawCSV, row)
	}
	return rawCSV
}

func ModifiedCSV(orig [][]string, mPercent int) [][]string {
	res := [][]string{}
	for i := 0; i < len(orig); i++ {
		row := append([]string{}, orig[i]...)
		if rand.Intn(100) <= mPercent {
			j := rand.Intn(len(row))
			row[j] = BrokenRandomAlphaNumericString(5)
		}
		res = append(res, row)
	}
	return res
}

func RawCSVReader(r [][]string) *csv.Reader {
	l := []string{}
	for _, o := range r {
		l = append(l, strings.Join(o, ","))
	}
	buf := bytes.NewBuffer([]byte(strings.Join(l, "\n")))
	return csv.NewReader(buf)
}
