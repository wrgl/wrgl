package table

import (
	"github.com/wrgl/core/pkg/kv"
)

func rowKey(sum []byte) []byte {
	return append([]byte("rows/"), sum...)
}

func SaveRow(s kv.DB, k, v []byte) error {
	return s.Set(rowKey(k), v)
}

func GetRow(s kv.DB, k []byte) ([]byte, error) {
	return s.Get(rowKey(k))
}

func GetRows(s kv.DB, keys [][]byte) ([][]byte, error) {
	result := [][]byte{}
	for _, k := range keys {
		v, err := s.Get(rowKey(k))
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

func DeleteRow(s kv.DB, k []byte) error {
	return s.Delete(rowKey(k))
}
