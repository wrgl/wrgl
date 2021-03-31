package table

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
)

var rowPrefix = []byte("rows/")

func rowKey(sum []byte) []byte {
	return append(rowPrefix, sum...)
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

func GetAllRowKeys(db kv.DB) ([]string, error) {
	sl, err := db.FilterKey(rowPrefix)
	if err != nil {
		return nil, err
	}
	l := len(rowPrefix)
	result := []string{}
	for _, h := range sl {
		result = slice.InsertToSortedStringSlice(result, h[l:])
	}
	return result, nil
}
