package versioning

import (
	"bytes"
	"encoding/gob"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
)

type Repo struct {
	TableStoreType table.StoreType
	CommitHash     string
}

const repoKey = "repo"

func (c *Repo) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *Repo) Save(s kv.DB) error {
	v, err := r.encode()
	if err != nil {
		return err
	}
	err = s.Set([]byte(repoKey), v)
	if err != nil {
		return err
	}
	return nil
}

func decodeRepo(data []byte) (*Repo, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &Repo{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func GetRepo(s kv.DB) (*Repo, error) {
	v, err := s.Get([]byte(repoKey))
	if err != nil {
		return nil, err
	}
	var r *Repo
	r, err = decodeRepo(v)
	if err != nil {
		return nil, err
	}
	return r, nil
}
