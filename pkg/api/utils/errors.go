// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiutils

import (
	"encoding/hex"
	"fmt"
	"strings"
)

type UnrecognizedWantsError struct {
	sums [][]byte
}

func (err *UnrecognizedWantsError) Error() string {
	sums := []string{}
	for _, sum := range err.sums {
		sums = append(sums, hex.EncodeToString(sum))
	}
	return fmt.Sprintf("unrecognized wants: %s", strings.Join(sums, ", "))
}
