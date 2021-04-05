package table

import (
	"bytes"
	"encoding/csv"
	"io"
	"strconv"

	"github.com/mmcloughlin/meow"
)

func hashTable(seed uint64, columns []string, primaryKeyIndices []uint32, rowHashReader RowHashReader) ([]byte, error) {
	h := meow.New(seed)
	buf := bytes.NewBufferString("")
	writer := csv.NewWriter(buf)
	err := writer.Write(columns)
	if err != nil {
		return nil, err
	}
	pkSl := []string{}
	for _, v := range primaryKeyIndices {
		pkSl = append(pkSl, strconv.Itoa(int(v)))
	}
	err = writer.Write(pkSl)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	_, err = h.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		pkHash, rowHash, err := rowHashReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		buf.Reset()
		buf.Write(pkHash)
		buf.Write(rowHash)
		buf.WriteByte(byte('\n'))
		_, err = h.Write(buf.Bytes())
		if err != nil {
			return nil, err
		}
	}

	return h.Sum(nil), nil
}
