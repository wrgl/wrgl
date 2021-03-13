package encoding

import (
	"bytes"
	"encoding/csv"
)

func EncodeStrings(s []string) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	writer := csv.NewWriter(buf)
	err := writer.Write(s)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return buf.Bytes(), nil
}

func DecodeStrings(h []byte) ([]string, error) {
	buf := bytes.NewBuffer(h)
	reader := csv.NewReader(buf)
	return reader.Read()
}

func BatchDecodeStrings(s [][]byte) ([][]string, error) {
	var buf = bytes.NewBuffer([]byte{})
	var reader = csv.NewReader(buf)
	var res = [][]string{}
	for _, v := range s {
		_, err := buf.Write(v)
		if err != nil {
			return nil, err
		}
		r, err := reader.Read()
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
