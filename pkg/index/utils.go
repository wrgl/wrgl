// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package index

import (
	"encoding/binary"
	"io"
)

func writeUint32s(w io.Writer, sl []uint32) error {
	b := make([]byte, 4)
	for _, u := range sl {
		binary.BigEndian.PutUint32(b, u)
		_, err := w.Write(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func readUint32(r io.ReadSeeker, buf []byte, startOff int64, off int) (u uint32, err error) {
	_, err = r.Seek(startOff+int64(off)*4, io.SeekStart)
	if err != nil {
		return
	}
	_, err = r.Read(buf[:4])
	if err != nil {
		return
	}
	u = binary.BigEndian.Uint32(buf)
	return
}

func readHash(r io.ReadSeeker, buf []byte, startOff int64, off int) (b []byte, err error) {
	_, err = r.Seek(startOff+int64(off)*16, io.SeekStart)
	if err != nil {
		return
	}
	_, err = r.Read(buf[:16])
	if err != nil {
		return
	}
	return buf[:16], nil
}

func writeHash(r io.WriteSeeker, startOff int64, off int, b []byte) (err error) {
	_, err = r.Seek(startOff+int64(off)*16, io.SeekStart)
	if err != nil {
		return
	}
	_, err = r.Write(b)
	return err
}
