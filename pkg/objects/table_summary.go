// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
)

type ColumnSummary struct {
	Name      string      `json:"name"`
	NullCount uint32      `json:"nullCount"`
	IsNumber  bool        `json:"isNumber,omitempty"`
	Min       *float64    `json:"min,omitempty"`
	Max       *float64    `json:"max,omitempty"`
	AvgStrLen uint16      `json:"avgStrLen"`
	TopValues ValueCounts `json:"topValues,omitempty"`
}

type summaryField struct {
	Name    string
	Write   func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error)
	IsEmpty func(col *ColumnSummary) bool
	Read    func(p *encoding.Parser, col *ColumnSummary) (int64, error)
}

var (
	summaryFields   []*summaryField
	summaryFieldMap map[string]*summaryField
)

func init() {
	summaryFields = []*summaryField{
		{
			Name: "name",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteString(w, buf, col.Name)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.Name == ""
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return objline.ReadString(p, &col.Name)
			},
		},
		{
			Name: "nullCount",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteUint32(w, buf, col.NullCount)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.NullCount == 0
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return objline.ReadUint32(p, &col.NullCount)
			},
		},
		{
			Name: "isNumber",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteBool(w, buf, col.IsNumber)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return !col.IsNumber
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return objline.ReadBool(p, &col.IsNumber)
			},
		},
		{
			Name: "min",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteFloat64(w, buf, *col.Min)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.Min == nil
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				if col.Min == nil {
					var f float64
					col.Min = &f
				}
				return objline.ReadFloat64(p, col.Min)
			},
		},
		{
			Name: "max",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteFloat64(w, buf, *col.Max)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.Max == nil
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				if col.Max == nil {
					var f float64
					col.Max = &f
				}
				return objline.ReadFloat64(p, col.Max)
			},
		},
		{
			Name: "avgStrLen",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteUint16(w, buf, col.AvgStrLen)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.AvgStrLen == 0
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return objline.ReadUint16(p, &col.AvgStrLen)
			},
		},
		{
			Name: "topValues",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return writeValueCounts(w, buf, col.TopValues)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.TopValues == nil
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return readValueCounts(p, &col.TopValues)
			},
		},
	}
	summaryFieldMap = map[string]*summaryField{}
	for _, f := range summaryFields {
		summaryFieldMap[f.Name] = f
	}
}

type TableSummary struct {
	RowsCount uint32           `json:"rowsCount"`
	Columns   []*ColumnSummary `json:"columns"`
}

func (t *TableSummary) WriteTo(w io.Writer) (total int64, err error) {
	buf := misc.NewBuffer(nil)
	names := make([]string, len(summaryFields))
	for i, f := range summaryFields {
		names[i] = f.Name
	}
	for _, field := range []fieldEncode{
		{"fields", objline.WriteBytes(NewStrListEncoder(true).Encode(names))},
		{"rowsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, t.RowsCount)
		}},
		{"colsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, uint32(len(t.Columns)))
		}},
		{"columns", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			for _, col := range t.Columns {
				for j, field := range summaryFields {
					// skip empty field
					if field.IsEmpty(col) {
						continue
					}
					// write the field index
					l, err := objline.WriteUint16(w, buf, uint16(j+1))
					if err != nil {
						return 0, err
					}
					n += l
					// write the field content
					l, err = field.Write(w, buf, col)
					if err != nil {
						return 0, err
					}
					n += l
				}
				// mark the end of a column
				l, err := objline.WriteUint16(w, buf, 0)
				if err != nil {
					return 0, err
				}
				n += l
			}
			return n, nil
		}},
	} {
		n, err := objline.WriteField(w, buf, field.label, field.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (t *TableSummary) ReadFrom(r io.Reader) (total int64, err error) {
	parser := encoding.NewParser(r)
	var fields []string
	var count uint32
	for _, f := range []fieldDecode{
		{"fields", func(p *encoding.Parser) (n int64, err error) {
			n, fields, err = NewStrListDecoder(false).Read(p)
			if err != nil {
				return 0, err
			}
			return n, nil
		}},
		{"rowsCount", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &t.RowsCount)
		}},
		{"colsCount", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &count)
		}},
		{"columns", func(p *encoding.Parser) (n int64, err error) {
			var j uint16
			nFields := uint16(len(fields))
			t.Columns = make([]*ColumnSummary, count)
			for i := uint32(0); i < count; i++ {
				t.Columns[i] = &ColumnSummary{}
				for {
					l, err := objline.ReadUint16(p, &j)
					if err != nil {
						return 0, err
					}
					n += l
					if j == 0 {
						break
					}
					if j > nFields {
						return 0, p.ParseError("invalid field index %d >= %d", j, nFields)
					}
					field := fields[j-1]
					if sf, ok := summaryFieldMap[field]; !ok {
						return 0, p.ParseError("summary field %q not found", field)
					} else {
						l, err = sf.Read(p, t.Columns[i])
						if err != nil {
							return 0, err
						}
						n += l
					}
				}
			}
			return
		}},
	} {
		n, err := objline.ReadField(parser, f.label, f.f)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return
}
