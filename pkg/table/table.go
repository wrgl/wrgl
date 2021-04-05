package table

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"io"

	"github.com/mmcloughlin/meow"
)

func hashTable(seed uint64, columns []string, primaryKeyIndices []uint32, rowHashReader RowHashReader) (string, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	h := meow.New(seed)

	err := encoder.Encode(columns)
	if err != nil {
		return "", err
	}
	_, err = h.Write(buf.Bytes())
	if err != nil {
		return "", err
	}

	buf.Reset()
	err = encoder.Encode(primaryKeyIndices)
	if err != nil {
		return "", err
	}
	_, err = h.Write(buf.Bytes())
	if err != nil {
		return "", err
	}

	for {
		pkHash, rowHash, err := rowHashReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		buf.Reset()
		buf.Write(pkHash)
		buf.Write(rowHash)
		buf.WriteByte(byte('\n'))
		_, err = h.Write(buf.Bytes())
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
