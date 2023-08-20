// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package index

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

type ReadWriteSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
	io.Writer
}

// HashSet is an high performance lookup table which can act
// like a set of 16 bytes slices. It borrows ideas from Git's
// packfile index such as having a fanout table to reduce
// lookup time.
type HashSet struct {
	fanout    [256]uint32
	r         ReadWriteSeekCloser
	size      uint32
	batchSize uint32
	batch     [][]byte
	buf       []byte
	mutex     sync.Mutex
}

const defaultBatchSize = 1024

// NewHashSet creates a new HashSet struct. r is the file it will read/write to.
// batchSize is the size of insertion batch. batchSize = 1 means all insertions
// are instantly written to disk. If batchSize = 0 then it will be set to 1024.
func NewHashSet(r ReadWriteSeekCloser, batchSize uint32) (s *HashSet, err error) {
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}
	s = &HashSet{
		r:         r,
		batchSize: batchSize,
		buf:       make([]byte, 16),
	}
	getSize := true
	for i := 0; i < 256; i++ {
		s.fanout[i], err = readUint32(s.r, s.buf, 0, i)
		if errors.Is(err, io.EOF) {
			getSize = false
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if getSize {
		s.size, err = readUint32(s.r, s.buf, 0, 255)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *HashSet) Len() int {
	return int(s.size)
}

// Add adds new slice of 16 bytes to this set. It will do nothing
// if it already has this slice. After all values are added, make
// sure to call Flush to persist all additions.
func (s *HashSet) Add(hash []byte) error {
	off, err := indexOf(s.r, s.buf, hash)
	if err != nil {
		return fmt.Errorf("indexOf error: %v", err)
	}
	if off != -1 {
		return nil
	}
	s.batch = append(s.batch, hash)
	if len(s.batch) >= int(s.batchSize) {
		return s.Flush()
	}
	return nil
}

type insertGroup struct {
	Off    int
	Hashes [][]byte
}

func (s *HashSet) addToHashTable() error {
	// group insertions based on offset at which to insert
	groupMap := map[int]*insertGroup{}
	groups := []*insertGroup{}
	n := 0
	for _, b := range s.batch {
		off, err := insertIndex(s.r, s.buf, b)
		if err != nil {
			return err
		}
		if v, ok := groupMap[off]; ok {
			v.Hashes = append(v.Hashes, b)
		} else {
			groupMap[off] = &insertGroup{
				Off:    off,
				Hashes: [][]byte{b},
			}
			groups = append(groups, groupMap[off])
		}
		n++
	}

	// sort groups by insert offset descending
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Off > groups[j].Off
	})

	end := s.Len()
	dst := end + n
	// for each insert group
	for _, obj := range groups {
		// sort hashes in this group in ascending order
		sort.Slice(obj.Hashes, func(i, j int) bool {
			for k, b := range obj.Hashes[i] {
				if b < obj.Hashes[j][k] {
					return true
				} else if b > obj.Hashes[j][k] {
					return false
				}
			}
			return false
		})
		l := len(obj.Hashes)
		// copy values above offset to new place
		for i := end - 1; i >= obj.Off; i-- {
			h, err := readHash(s.r, s.buf, 1024, i)
			if err != nil {
				return err
			}
			err = writeHash(s.r, 1024, dst-end+i, h)
			if err != nil {
				return err
			}
		}
		// insert new values
		dst = dst - end + obj.Off - l
		for i, b := range obj.Hashes {
			err := writeHash(s.r, 1024, dst+i, b)
			if err != nil {
				return err
			}
		}
		end = obj.Off
	}
	return nil
}

// Flush flushes all pending insertions to disk.
func (s *HashSet) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.addToHashTable()
	if err != nil {
		return err
	}
	addToFanoutTable(&s.fanout, s.batch)
	_, err = s.r.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = writeUint32s(s.r, s.fanout[:])
	if err != nil {
		return err
	}
	s.size += uint32(len(s.batch))
	s.batch = s.batch[:0]
	return nil
}

func (s *HashSet) Close() error {
	return s.r.Close()
}

func (s *HashSet) Has(b []byte) (bool, error) {
	pos, err := indexOf(s.r, s.buf, b)
	if err != nil {
		return false, err
	}
	return pos != -1, nil
}
