package versioning

import (
	"fmt"
	"io"

	"github.com/mmcloughlin/meow"

	"github.com/wrgl/core/pkg/encoding"
)

func IngestCSV(reader CSVReader, primaryKeys []string, seed uint64) (*Table, map[string][]byte, error) {
	row, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("reader.Read columns error: %v", err)
	}
	columns := make([]string, len(row))
	copy(columns, row)

	var ki []int
	ki, err = KeyIndices(columns, primaryKeys)
	if err != nil {
		return nil, nil, fmt.Errorf("KeyIndices error: %v", err)
	}

	t := &Table{
		Columns:     columns,
		PrimaryKeys: ki,
	}

	rows := map[string][]byte{}
	rowMap := map[string][]byte{}
	rowNum := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, fmt.Errorf("reader.Read row error: %v", err)
		}

		var vb []byte
		vb, err = encoding.EncodeStrings(row)
		if err != nil {
			return nil, nil, fmt.Errorf("EncodeStrings row error: %v", err)
		}

		var kb []byte
		if len(ki) > 0 {
			kb, err = encoding.EncodeStrings(IndicesToValues(row, ki))
			if err != nil {
				return nil, nil, fmt.Errorf("EncodeStrings pk error: %v", err)
			}
		} else {
			kb = vb
		}

		keysHash := meow.Checksum(seed, kb)
		valsHash := meow.Checksum(seed, vb)

		keyStr := string(keysHash[:])
		rowNum++

		if v, ok := rows[keyStr]; ok {
			delete(rowMap, string(v))
		}

		rowMap[string(valsHash[:])] = vb
		rows[keyStr] = valsHash[:]
		t.Rows = append(t.Rows, KeyHash{K: keyStr, V: valsHash[:]})
	}

	return t, rowMap, nil
}
