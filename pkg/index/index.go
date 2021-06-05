package index

import (
	"io"
	"sort"
)

func insertIndex(r io.ReadSeeker, buf, b []byte) (off int, err error) {
	// read fanout table to get start index and end index
	var startInd, endInd uint32
	if b[0] > 0 {
		startInd, err = readUint32(r, buf, 0, int(b[0]-1))
		if err == io.EOF {
			return 0, nil
		}
		if err != nil {
			return
		}
	}
	endInd, err = readUint32(r, buf, 0, int(b[0]))
	if err != nil {
		return
	}
	if startInd == endInd {
		return int(startInd), nil
	}

	// search the hashes
	searchSize := int(endInd - startInd)
	pos := sort.Search(searchSize, func(pos int) bool {
		h, err := readHash(r, buf, 1024, int(startInd)+pos)
		if err != nil {
			panic(err)
		}
		for k := 0; k < 16; k++ {
			if h[k] < b[k] {
				return false
			} else if h[k] > b[k] {
				return true
			}
		}
		return true
	})
	return pos + int(startInd), nil
}

func hashAtIndexEqual(r io.ReadSeeker, buf []byte, off int, b []byte) (bool, error) {
	h, err := readHash(r, buf, 1024, off)
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for k := 0; k < 16; k++ {
		if h[k] != b[k] {
			return false, nil
		}
	}
	return true, nil
}

func indexOf(r io.ReadSeeker, buf, b []byte) (off int, err error) {
	pos, err := insertIndex(r, buf, b)
	if err != nil {
		return
	}

	ok, err := hashAtIndexEqual(r, buf, pos, b)
	if err != nil {
		return
	}
	if !ok {
		return -1, nil
	}

	return pos, nil
}
