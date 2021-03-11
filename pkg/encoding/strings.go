package encoding

import (
	"bytes"
	"encoding/csv"
	"io/ioutil"
)

func EncodeStrings(s []string) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	writer := csv.NewWriter(buf)
	err := writer.Write(s)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	b, err := ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}
	return b[:len(b)-1], nil
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
