// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils

type Object struct {
	Type    int
	Content []byte
	Sum     []byte
}
