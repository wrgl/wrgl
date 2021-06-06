package merge

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/wrgl/core/pkg/index"
	"github.com/wrgl/core/pkg/objects"
)

// SortableRows is a high performance data structure that store a table
// (rows x columns) of values. As the name suggest, this structure also
// support sorting of rows based on a customizable number of columns in
// either ascending or descending order. SortableRows implements
// sort.Interface so you can call sort.Sort on it.
type SortableRows struct {
	rows    index.ReadWriteSeekCloser
	offsets index.ReadWriteSeekCloser
	enc     *objects.StrListEncoder
	dec     *objects.StrListDecoder
	ncols   int
	sortBy  []int
	off     int
	buf     []byte
	size    uint32
	mutex   sync.Mutex
}

// NewSortableRows creates a new SortableRows struct. It stores data in
// 2 separate files: rows and offsets. sortBy is the column indices to
// sort by. Note that indices in sortBy are 1-based instead of 0-based.
// Pass in 0 will return an error. if index is positive then sort
// ascending, otherwise sort descending.
func NewSortableRows(rows, offsets index.ReadWriteSeekCloser, sortBy []int) (r *SortableRows, err error) {
	for _, i := range sortBy {
		if i == 0 {
			return nil, fmt.Errorf("bad sortBy index 0, if you want to sort by first collumn ascending then pass in 1 instead")
		}
	}
	r = &SortableRows{
		rows:    rows,
		offsets: offsets,
		sortBy:  sortBy,
		enc:     objects.NewStrListEncoder(),
		dec:     objects.NewStrListDecoder(false),
		buf:     make([]byte, 8),
	}
	r.readSize()
	if r.size > 0 {
		o, err := r.rowOffset(int(r.size))
		if err != nil {
			return nil, err
		}
		r.off = int(o)
		_, err = r.rows.Seek(int64(r.off), io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *SortableRows) SetSortBy(sortBy []int) error {
	for _, i := range sortBy {
		if i == 0 {
			return fmt.Errorf("bad sortBy index 0, if you want to sort by first collumn ascending then pass in 1 instead")
		}
	}
	r.sortBy = sortBy
	return nil
}

// AddBytes add already encoded string slice bytes produced
// by objects.StrListEncoder.
func (r *SortableRows) AddBytes(b []byte) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	n, err := r.rows.Write(b)
	if err != nil {
		return err
	}
	r.off += n
	r.size++
	r.putRowOffset(int(r.size), int64(r.off))
	if err != nil {
		return err
	}
	return r.putSize()
}

// Add adds string slice to this table, ensuring number of columns
// equal number of columns in rows it has seen so far.
func (r *SortableRows) Add(row []string) (err error) {
	if r.ncols == 0 {
		r.ncols = len(row)
	} else if r.ncols != len(row) {
		return fmt.Errorf("number of columns not matching: %d != %d", len(row), r.ncols)
	}
	return r.AddBytes(r.enc.Encode(row))
}

func (r *SortableRows) Len() int {
	return int(r.size)
}

func (r *SortableRows) readSize() (err error) {
	_, err = r.offsets.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	_, err = r.offsets.Read(r.buf[:4])
	if err != nil {
		return
	}
	r.size = binary.BigEndian.Uint32(r.buf)
	return nil
}

func (r *SortableRows) putSize() (err error) {
	_, err = r.offsets.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	binary.BigEndian.PutUint32(r.buf, r.size)
	_, err = r.offsets.Write(r.buf[:4])
	return err
}

func (r *SortableRows) putRowOffset(i int, off int64) (err error) {
	_, err = r.offsets.Seek(4+(int64(i)*8), io.SeekStart)
	if err != nil {
		return
	}
	binary.BigEndian.PutUint64(r.buf, uint64(off))
	_, err = r.offsets.Write(r.buf[:8])
	return err
}

func (r *SortableRows) rowOffset(i int) (off int64, err error) {
	_, err = r.offsets.Seek(4+(int64(i)*8), io.SeekStart)
	if err != nil {
		return
	}
	_, err = r.offsets.Read(r.buf[:8])
	if err != nil {
		return
	}
	u := binary.BigEndian.Uint64(r.buf)
	return int64(u), nil
}

func (r *SortableRows) readCell(off int64, j uint32) (v string, err error) {
	_, err = r.rows.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	return r.dec.ReadColumn(r.rows, j)
}

func (r *SortableRows) Less(i, j int) bool {
	offi, err := r.rowOffset(i)
	if err != nil {
		panic(err)
	}
	offj, err := r.rowOffset(j)
	if err != nil {
		panic(err)
	}
	for _, c := range r.sortBy {
		k := uint32(c) - 1
		if c < 0 {
			k = uint32(-c) - 1
		}
		vi, err := r.readCell(offi, k)
		if err != nil {
			panic(fmt.Errorf("error reading cell (%x, %d): %v", offi, k, err))
		}
		vj, err := r.readCell(offj, k)
		if err != nil {
			panic(fmt.Errorf("error reading cell (%x, %d): %v", offj, k, err))
		}
		if vi < vj {
			return c > 0
		} else if vi > vj {
			return c < 0
		}
	}
	return false
}

func (r *SortableRows) Swap(i, j int) {
	offi, err := r.rowOffset(i)
	if err != nil {
		panic(err)
	}
	offj, err := r.rowOffset(j)
	if err != nil {
		panic(err)
	}
	err = r.putRowOffset(i, offj)
	if err != nil {
		panic(err)
	}
	err = r.putRowOffset(j, offi)
	if err != nil {
		panic(err)
	}
}

func (r *SortableRows) Close() error {
	err := r.rows.Close()
	if err != nil {
		return err
	}
	return r.offsets.Close()
}

// RowsChan creates a new channel to consume string slices
// from this table
func (r *SortableRows) RowsChan(errChan chan<- error) <-chan []string {
	ch := make(chan []string)
	go func() {
		defer close(ch)
		_, err := r.offsets.Seek(4, io.SeekStart)
		if err != nil {
			errChan <- err
			return
		}
		for i := 0; i < int(r.size); i++ {
			_, err := r.offsets.Read(r.buf[:8])
			if err != nil {
				errChan <- err
				return
			}
			off := binary.BigEndian.Uint64(r.buf)
			_, err = r.rows.Seek(int64(off), io.SeekStart)
			if err != nil {
				errChan <- err
				return
			}
			_, sl, err := r.dec.Read(r.rows)
			if err != nil {
				errChan <- err
				return
			}
			ch <- sl
		}
	}()
	return ch
}
