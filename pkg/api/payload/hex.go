// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

import (
	"encoding/hex"
	"fmt"
)

type Hex [16]byte

func (x *Hex) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%x"`, *x)), nil
}

func (x *Hex) UnmarshalJSON(b []byte) error {
	b = b[1 : len(b)-1]
	_, err := hex.Decode((*x)[:], b)
	return err
}

func AppendHex(sl []*Hex, b []byte) []*Hex {
	h := &Hex{}
	copy((*h)[:], b)
	return append(sl, h)
}

func HexSliceToBytesSlice(sl []*Hex) [][]byte {
	b := make([][]byte, len(sl))
	for i, v := range sl {
		b[i] = (*v)[:]
	}
	return b
}

func BytesToHex(b []byte) *Hex {
	if b == nil {
		return nil
	}
	h := &Hex{}
	copy((*h)[:], b)
	return h
}
