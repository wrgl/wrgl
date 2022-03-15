// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
)

func SeekCommonAncestor(db objects.Store, commits ...[]byte) (baseCommit []byte, err error) {
	n := len(commits)
	qs := make([]*CommitsQueue, n)
	bases := make([][]byte, n)
	for i, sum := range commits {
		qs[i], err = NewCommitsQueue(db, [][]byte{sum})
		if err != nil {
			return
		}
		bases[i] = sum
	}
	for {
		for i := len(bases) - 1; i >= 0; i-- {
			for j := len(bases) - 1; j >= 0; j-- {
				if i == j {
					continue
				}
				if qs[j].Seen(bases[i]) {
					// remove j element
					copy(bases[j:], bases[j+1:])
					bases = bases[:len(bases)-1]
					copy(qs[j:], qs[j+1:])
					qs = qs[:len(qs)-1]
					if i > j {
						i--
					}
				}
			}
		}
		if len(bases) == 1 {
			break
		}
		eofs := 0
		for i, q := range qs {
			bases[i], _, err = q.PopInsertParents()
			if err == io.EOF {
				eofs++
			} else if err != nil {
				return nil, err
			}
		}
		if eofs == len(qs) {
			return nil, fmt.Errorf("common ancestor commit not found")
		}
	}
	return bases[0], nil
}
