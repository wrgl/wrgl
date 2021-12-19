// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
)

type ValueCount struct {
	Value string `json:"v"`
	Count uint32 `json:"c"`
}

type ValueCounts []ValueCount

func (a ValueCounts) Len() int           { return len(a) }
func (a ValueCounts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ValueCounts) Less(i, j int) bool { return a[i].Count > a[j].Count }

func (a ValueCounts) IsEmpty() bool {
	return a == nil
}

func writeValueCounts(w io.Writer, buf encoding.Bufferer, a ValueCounts) (int64, error) {
	b := buf.Buffer(4)
	binary.BigEndian.PutUint32(b, uint32(a.Len()))
	n, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	total := int64(n)
	for _, vc := range a {
		binary.BigEndian.PutUint32(b, uint32(vc.Count))
		n, err := w.Write(b)
		if err != nil {
			return 0, err
		}
		total += int64(n)

		l := uint16(len(vc.Value))
		binary.BigEndian.PutUint16(b, l)
		n, err = w.Write(b[:2])
		if err != nil {
			return 0, err
		}
		total += int64(n)

		n, err = w.Write([]byte(vc.Value))
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

func readValueCounts(p *encoding.Parser, a *ValueCounts) (int64, error) {
	b, err := p.NextBytes(4)
	if err != nil {
		return 0, p.ParseError("error reading number of values: %v", err)
	}
	var total int64 = 4
	n := binary.BigEndian.Uint32(b)
	*a = make(ValueCounts, n)
	for i := uint32(0); i < n; i++ {
		b, err = p.NextBytes(4)
		if err != nil {
			return 0, p.ParseError("error reading value count: %v", err)
		}
		total += 4
		count := binary.BigEndian.Uint32(b)
		b, err = p.NextBytes(2)
		if err != nil {
			return 0, p.ParseError("error reading value length: %v", err)
		}
		total += 2
		l := binary.BigEndian.Uint16(b)
		b, err = p.NextBytes(int(l))
		if err != nil {
			return 0, p.ParseError("error reading value: %v", err)
		}
		total += int64(l)
		(*a)[i] = ValueCount{
			Count: count,
			Value: string(b),
		}
	}
	return total, nil
}
