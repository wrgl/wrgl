package table

import (
	"fmt"
	"io"
)

type smallRowReader struct {
	store *SmallStore
	off   int
	limit int
}

func (r *smallRowReader) Read() (rowHash, rowContent []byte, err error) {
	if r.off >= r.limit {
		return nil, nil, io.EOF
	}
	kh := r.store.table.Rows[r.off]
	rc, err := GetRow(r.store.db, kh.V)
	if err != nil {
		return nil, nil, err
	}
	r.off++
	return kh.V, rc, nil
}

func (r *smallRowReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.limit
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func (r *smallRowReader) ReadAt(offset int) (rowHash, rowContent []byte, err error) {
	kh := r.store.table.Rows[offset]
	rc, err := GetRow(r.store.db, kh.V)
	if err != nil {
		return nil, nil, err
	}
	return kh.V, rc, nil
}

func (r *smallRowReader) Close() error {
	return nil
}
