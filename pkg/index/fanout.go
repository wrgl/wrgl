package index

func computeFanoutTable(fanout *[256]uint32, hashes [][]byte) {
	if len(hashes) == 0 {
		return
	}
	var b uint8 = hashes[0][0]
	for i, s := range hashes {
		if s[0] > b {
			for k := b; k < s[0]; k++ {
				fanout[k] = uint32(i)
			}
			b = s[0]
		}
	}
	n := uint32(len(hashes))
	for k := int(b); k < 256; k++ {
		fanout[k] = n
	}
}

func addToFanoutTable(fanout *[256]uint32, hashes [][]byte) {
	m := map[byte]uint32{}
	for _, b := range hashes {
		m[b[0]]++
	}
	for b, u := range m {
		for k := b; ; k++ {
			fanout[k] += u
			if k == 255 {
				break
			}
		}
	}
}
