package table

import (
	"encoding/hex"

	"github.com/wrgl/core/pkg/kv"
)

func rowKey(hash string) []byte {
	return []byte("rows/" + hash)
}

func SaveRow(s kv.DB, k, v []byte) error {
	h := hex.EncodeToString([]byte(k))
	return s.Set(rowKey(h), v)
}

func GetRows(s kv.DB, keys [][]byte) ([][]byte, error) {
	result := [][]byte{}
	for _, k := range keys {
		hStr := hex.EncodeToString(k)
		v, err := s.Get(rowKey(hStr))
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

func DeleteRow(s kv.DB, k []byte) error {
	return s.Delete(rowKey(hex.EncodeToString(k)))
}
