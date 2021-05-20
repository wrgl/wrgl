// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils

type Update struct {
	OldSum []byte
	Sum    []byte
	Src    string
	Dst    string
	Force  bool
	ErrMsg string
}
