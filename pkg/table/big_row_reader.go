package table

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
)

type bigRowReader struct {
	db    kv.Store
	r     kv.File
	off   int
	limit int
}

func (r *bigRowReader) Read() (rowHash, rowContent []byte, err error) {
	if r.off >= r.limit {
		return nil, nil, io.EOF
	}
	sl := make([]byte, 32)
	_, err = r.r.Read(sl)
	if err != nil {
		return nil, nil, err
	}
	rowHash = sl[16:]
	rowContent, err = r.db.Get(rowKey(rowHash))
	if err != nil {
		return nil, nil, err
	}
	r.off++
	return
}

func (r *bigRowReader) Seek(offset int, whence int) (int, error) {
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
	_, err := r.r.Seek(int64(r.off*32), io.SeekStart)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (r *bigRowReader) ReadAt(offset int) (rowHash, rowContent []byte, err error) {
	sl := make([]byte, 32)
	_, err = r.r.ReadAt(sl, int64(offset*32))
	if err != nil {
		return nil, nil, err
	}
	rowHash = sl[16:]
	rowContent, err = r.db.Get(rowKey(rowHash))
	if err != nil {
		return nil, nil, err
	}
	return
}

func (r *bigRowReader) Close() error {
	return r.r.Close()
}
