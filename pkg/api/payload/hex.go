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
