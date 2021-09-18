package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/objects"
)

func findOverlappingBlocks(tblIdx1 [][]string, tblIdx2 [][]string, off1, prevEnd int) (start, end int) {
	n := len(tblIdx2)

	// find starting block in table 2
	start = -1
	if prevEnd == 0 {
		prevEnd++
	}
findStart:
	for j := prevEnd - 1; j < n; j++ {
		for k, s := range tblIdx1[off1] {
			if tblIdx2[j][k] > s {
				if j == 0 {
					start = j
				} else {
					start = j - 1
				}
				break findStart
			} else if tblIdx2[j][k] < s {
				continue findStart
			}
		}
		start = j
		break
	}
	if start == -1 {
		return n - 1, n
	}

	// find end block in table 2
	end = -1
	if off1 < len(tblIdx1)-1 {
	findEnd:
		for j := start; j < n; j++ {
			for k, s := range tblIdx1[off1+1] {
				if tblIdx2[j][k] > s {
					end = j
					break findEnd
				} else if tblIdx2[j][k] < s {
					continue findEnd
				}
			}
			end = j
			break
		}
	}
	if end == -1 {
		end = n
	}

	return
}

func getBlockIndices(db objects.Store, tbl *objects.Table, start, end int, prevSl []*objects.BlockIndex, prevStart, prevEnd int) ([]*objects.BlockIndex, error) {
	if start >= len(tbl.Blocks) || start == end {
		return nil, nil
	}
	sl := make([]*objects.BlockIndex, end-start)
	slStart := start
	if prevEnd > start {
		copy(sl, prevSl[start-prevStart:])
		start = prevEnd
	}
	var err error
	for j := start; j < end; j++ {
		sl[j-slStart], err = objects.GetBlockIndex(db, tbl.Blocks[j])
		if err != nil {
			return nil, err
		}
	}
	return sl, nil
}

// iterateAndMatch iterates through a single table while trying to match its rows with another table
func iterateAndMatch(db1, db2 objects.Store, tbl1, tbl2 *objects.Table, tblIdx1, tblIdx2 [][]string, debugOut io.Writer, cb func(pk, row1, row2 []byte, off1, off2 uint32)) error {
	var prevStart, prevEnd int
	var indices2 []*objects.BlockIndex
	for i, sum := range tbl1.Blocks {
		idx1, err := objects.GetBlockIndex(db1, sum)
		if err != nil {
			return err
		}
		start, end := findOverlappingBlocks(tblIdx1, tblIdx2, i, prevEnd)
		indices2, err = getBlockIndices(db2, tbl2, start, end, indices2, prevStart, prevEnd)
		if err != nil {
			return err
		}
		prevStart, prevEnd = start, end

		for rowOff1, b := range idx1.Rows {
			if debugOut != nil {
				fmt.Fprintf(debugOut, "%x:\n  [%d:%d] %x\n", b[:16], i, rowOff1, b[16:])
			}
			var row2 []byte
			var rowOff2 byte
			var blkOff2 uint32
			for k, idx := range indices2 {
				if off, sum := idx.Get(b[:16]); sum != nil {
					row2 = sum
					rowOff2 = off
					blkOff2 = uint32(k + start)
					if debugOut != nil {
						fmt.Fprintf(debugOut, "  [%d:%d] %x\n", blkOff2, rowOff2, row2)
					}
					break
				}
			}
			cb(b[:16], b[16:], row2, uint32(i*objects.BlockSize+rowOff1), blkOff2*objects.BlockSize+uint32(rowOff2))
		}
	}
	return nil
}
