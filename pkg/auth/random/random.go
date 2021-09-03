// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package random

import (
	"crypto/rand"
	"math/big"
	"unicode"
)

var runeRanges = []unicode.Range16{
	unicode.Digit.R16[0],
	unicode.Letter.R16[0],
	unicode.Lower.R16[0],
}

func RandomAlphaNumericString(n int) string {
	b := make([]byte, n)
	m := int64(len(runeRanges))
	for i := range b {
		j, err := rand.Int(rand.Reader, big.NewInt(m))
		if err != nil {
			panic(err)
		}
		r := runeRanges[j.Int64()]
		k, err := rand.Int(rand.Reader, big.NewInt(int64(r.Hi-r.Lo)))
		if err != nil {
			panic(err)
		}
		b[i] = byte(r.Lo + uint16(k.Int64())*r.Stride)
	}
	return string(b)
}
