// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package encoding

type Bufferer interface {
	Buffer(n int) []byte
}
