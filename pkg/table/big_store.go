package table

import (
	"bytes"
	"io"

	"github.com/google/uuid"
	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

var tempPrefix = []byte("tmp/")

func tempKey() ([]byte, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return append(tempPrefix, []byte(id.String())...), nil
}

type BigStore struct {
	t    *objects.Table
	fs   kv.FileStore
	seed uint64
}

func NewBigStore(fs kv.FileStore, t *objects.Table, seed uint64) *BigStore {
	return &BigStore{
		t:    t,
		fs:   fs,
		seed: seed,
	}
}

func (s *BigStore) Save() ([]byte, error) {
	key, err := tempKey()
	if err != nil {
		return nil, err
	}
	file, err := s.fs.Writer(key)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	buf := bytes.NewBufferString("")
	writer := objects.NewTableWriter(io.MultiWriter(file, buf))
	err = writer.Write(s.t)
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	sum := meow.Checksum(s.seed, buf.Bytes())
	err = s.fs.Move(key, smallTableKey(sum[:]))
	if err != nil {
		return nil, err
	}
	return sum[:], nil
}
