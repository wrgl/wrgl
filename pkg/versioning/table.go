package versioning

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"

	"github.com/mmcloughlin/meow"

	"github.com/wrgl/core/pkg/kv"
)

type KeyHash struct {
	K string
	V []byte
}

type Table struct {
	Columns     []string
	PrimaryKeys []int
	Rows        []KeyHash
}

func (t *Table) PrimaryKeyStrings() []string {
	return IndicesToValues(t.Columns, t.PrimaryKeys)
}

func (t *Table) RowsMap() map[string][]byte {
	res := make(map[string][]byte, len(t.Rows))
	for _, r := range t.Rows {
		res[r.K] = r.V
	}
	return res
}

func (t *Table) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func tablePrefix(orgID, repo string) []byte {
	return []byte(fmt.Sprintf("%s/%s/table/", orgID, repo))
}

func tableKey(orgID, repo, hash string) []byte {
	return append(tablePrefix(orgID, repo), []byte(hash)...)
}

func (t *Table) sum(seed uint64) (string, []byte, error) {
	v, err := t.encode()
	if err != nil {
		return "", nil, err
	}
	kb := meow.Checksum(seed, v)
	ks := hex.EncodeToString(kb[:])
	return ks, v, nil
}

func (t *Table) Save(s kv.DB, orgID, repo string, seed uint64) (string, error) {
	ks, v, err := t.sum(seed)
	if err != nil {
		return "", err
	}
	err = s.Set(tableKey(orgID, repo, ks), v)
	if err != nil {
		return "", err
	}
	return ks, nil
}

func decodeTable(data []byte) (*Table, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &Table{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func GetTable(s kv.DB, orgID, repo, hash string) (*Table, error) {
	v, err := s.Get(tableKey(orgID, repo, hash))
	if err != nil {
		return nil, err
	}
	var t *Table
	t, err = decodeTable(v)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func TableExist(s kv.DB, orgID, repo, hash string) bool {
	return s.Exist(tableKey(orgID, repo, hash))
}

func GetAllTableHashes(s kv.DB, orgID, repo string) ([]string, error) {
	prefix := tablePrefix(orgID, repo)
	l := len(prefix)
	sl, err := s.FilterKey(prefix)
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, k := range sl {
		result = append(result, k[l:])
	}
	return result, nil
}

func DeleteTable(s kv.DB, orgID, repo, hash string) error {
	return s.Delete(tableKey(orgID, repo, hash))
}

func DeleteAllTables(s kv.Store, orgID, repo string) error {
	return s.Clear(tablePrefix(orgID, repo))
}
